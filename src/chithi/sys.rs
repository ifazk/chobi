use chrono::{Datelike, Timelike};
use std::os::fd::{AsRawFd, RawFd};
use std::{ffi, io, process};

// Ah hostnames, what wonderful fun
// We make some simplifying support decisions here about the size of hostnames.
// In linux, sysconf(_SC_HOST_NAME_MAX) cannot return a value less than _POSIX_HOST_NAME_MAX (255).
// Source https://www.man7.org/linux/man-pages/man3/sysconf.3.html.
// In FreeBSD, callers to gethostname "should be aware that {HOST_NAME_MAX} may
// be variable or infinite, but is guaranteed to be no less than
// {_POSIX_HOST_NAME_MAX}."
// Source https://man.freebsd.org/cgi/man.cgi?query=gethostname.
// The above two sizes do not include the null terminating byte.
// Note: The size limit is larger than the limit on DNS names
// In the DNS world, RFC 1035 Section 2.3.4 says names are 255 octets or less,
// with length encoded labels ending with a root label length 0, similar to null
// termination. But the 255 size does include the terminating length 0 in the
// DNS case.
const CHITHI_HOST_NAME_MAX: usize = 255;

/// Returns an error if hostname is too long
pub fn hostname() -> io::Result<String> {
    // Add 1 for null terminator
    const NAMELEN: usize = CHITHI_HOST_NAME_MAX + 1;
    let mut buffer = [0 as libc::c_char; NAMELEN];
    let rc = unsafe { libc::gethostname(buffer.as_mut_ptr(), NAMELEN) };
    // Returns 0 or -1 (https://pubs.opengroup.org/onlinepubs/9799919799/functions/gethostname.html)
    if rc == -1 {
        return Err(io::Error::last_os_error());
    };
    // If NAMELEN is too small, then the hostname is truncated, and
    // null-termination isn't specified. The man pages also do not say whether
    // gethostname will return -1 or not if truncation happens. Just do a
    // null-termination check just to be sure.
    if !buffer.contains(&0) {
        return Err(io::Error::other("hostname longer than 255 bytes"));
    };
    // Safety: already did a null-termination check above, so safe
    let hostname_cstr = unsafe { ffi::CStr::from_ptr(buffer.as_ptr()) };
    let hostname = hostname_cstr
        .to_str()
        .map_err(|e| io::Error::other(format!("failed to obtain hostname from c string: {e}")))?;
    Ok(hostname.to_string())
}

/// Gets the date and time with a timezone offset
pub fn get_syncoid_date() -> String {
    let local = chrono::Local::now();
    let year = local.year();
    let mon = local.month();
    let mday = local.day();
    let hour = local.hour();
    let min = local.minute();
    let sec = local.second();
    let tz_offset = local.offset().local_minus_utc();
    let sign = if tz_offset < 0 { "-" } else { "" }; // + is not allowed in a snapshot name
    let tz_offset = tz_offset.unsigned_abs();
    let hours = tz_offset / 3600;
    let minutes = (tz_offset / 60) - (hours * 60);
    format!(
        "{year:04}-{mon:02}-{mday:02}:{hour:02}:{min:02}:{sec:02}-GMT{sign}{hours:02}:{minutes:02}"
    )
}

// Utility functions for captures
fn set_flags(fd: libc::c_int, flags: libc::c_int) -> io::Result<()> {
    unsafe {
        let ret = libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        if ret == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
fn get_not_blocking_fd<T: AsRawFd>(fd: &T) -> io::Result<RawFd> {
    let fd = fd.as_raw_fd();
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        set_flags(fd, flags | libc::O_NONBLOCK)?;
        Ok(fd)
    }
}

// Helpers for poll
fn poll_readable(revents: libc::c_short) -> bool {
    revents & libc::POLLIN != 0
}
fn poll_ended(revents: libc::c_short) -> bool {
    revents & libc::POLLHUP != 0
}

/// Run command and both prints and captures outputs (stdout and stderr)
pub fn capture(command: &mut process::Command) -> io::Result<process::Output> {
    use io::{Read, Write};
    use process::{ExitStatus, Stdio};

    let command = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Buffers
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    // Spawn child and collect outputs
    let status: ExitStatus = {
        let mut child = command.spawn()?;

        // Handles that implement Drop
        let mut child_out = child.stdout.take().expect("child stdout is piped");
        let mut child_err = child.stderr.take().expect("child stderr is piped");

        // Set child fds to non-blocking
        let child_out_fd = get_not_blocking_fd(&child_out)?;
        let child_err_fd = get_not_blocking_fd(&child_err)?;

        // Setup
        let mut pollfds = vec![
            libc::pollfd {
                fd: child_out_fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: child_err_fd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let mut idx_map = (0..pollfds.len()).collect::<Vec<_>>();
        let mut remove_buffer = Vec::new();
        let mut readbuf = [0u8; 1024];
        loop {
            if pollfds.is_empty() {
                break;
            }
            let ret =
                unsafe { libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, -1) };
            if ret == -1 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                } else {
                    return Err(err);
                }
            };

            for (pollfd_idx, pollfd) in pollfds.iter().enumerate() {
                let idx = idx_map[pollfd_idx];
                if idx == 0 {
                    // child_out
                    if poll_readable(pollfd.revents) {
                        match child_out.read(&mut readbuf) {
                            Ok(n) => {
                                stdout.extend_from_slice(&readbuf[..n]);
                                io::stdout().write_all(&readbuf[..n])?;
                            }
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            Err(e) => return Err(e),
                        }
                    } else if poll_ended(pollfd.revents) {
                        remove_buffer.push(pollfd_idx);
                    }
                } else {
                    // child_err
                    if poll_readable(pollfd.revents) {
                        match child_err.read(&mut readbuf) {
                            Ok(n) => {
                                stderr.extend_from_slice(&readbuf[..n]);
                                io::stderr().write_all(&readbuf[..n])?;
                            }
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            Err(e) => return Err(e),
                        }
                    } else if poll_ended(pollfd.revents) {
                        remove_buffer.push(pollfd_idx);
                    }
                }
            }

            if !remove_buffer.is_empty() {
                // sort so that removing later elements do not move earlier elements
                remove_buffer.sort();
                while let Some(idx) = remove_buffer.pop() {
                    pollfds.remove(idx);
                    idx_map.remove(idx);
                }
            }
        }

        child.wait()?
    };

    Ok(process::Output {
        status,
        stdout,
        stderr,
    })
}

pub fn try_terminate_if_running(child: &mut process::Child) -> io::Result<()> {
    match child.try_wait() {
        Ok(None) => {
            let id = child.id();
            unsafe {
                let ret = libc::kill(id as libc::pid_t, libc::SIGTERM);
                if ret == -1 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Run command and captures outputs. Also prints the errs if output_errs is true
pub fn pipe_and_capture_stderr(
    main: &mut process::Command,
    pv: Option<&mut process::Command>,
    other: &mut process::Command,
    output_errs: bool,
) -> io::Result<(process::Output, process::Output)> {
    use process::Stdio;

    let main = main
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut main_child = main.spawn()?;

    let pv = match pv {
        Some(pv) => {
            let pv = pv
                .stdin(Stdio::from(
                    main_child.stdout.take().expect("stdout is piped"),
                ))
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            Some(pv)
        }
        None => None,
    };
    let mut pv_child = match pv {
        Some(pv) => match pv.spawn() {
            Ok(child) => Some(child),
            Err(e) => {
                let _ = try_terminate_if_running(&mut main_child);
                main_child.wait()?;
                return Err(e);
            }
        },
        None => None,
    };

    let other_stdin = pv_child
        .as_mut()
        .map(|pv_child| pv_child.stdout.take().expect("stdout is piped"))
        .unwrap_or_else(|| main_child.stdout.take().expect("stdout is piped"));

    let other = other
        .stdin(Stdio::from(other_stdin))
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped());

    let mut other_child = match other.spawn() {
        Ok(child) => child,
        Err(e) => {
            if let Some(mut pv_child) = pv_child {
                let _ = try_terminate_if_running(&mut pv_child);
                pv_child.wait()?;
            };
            let _ = try_terminate_if_running(&mut main_child);
            main_child.wait()?;
            return Err(e);
        }
    };

    // Handles that implement Drop
    let main_err = main_child.stderr.take().expect("child stderr is piped");
    let other_err = other_child.stderr.take().expect("child stderr is piped");

    let mut children = vec![main_child, other_child];

    // Setup
    let mut outputs = read_until_hup(
        &mut children,
        &mut [main_err, other_err],
        4096,
        output_errs.then(io::stderr),
    )?;

    let pv_status = pv_child.as_mut().map(|pv_child| pv_child.wait());
    if let Some(pv_status) = pv_status {
        let pv_status = pv_status?;
        if !pv_status.success() {
            return Err(io::Error::other(format!("pv errored with {pv_status}")));
        }
    };

    let other = outputs.pop().expect("lengths should match");
    let other = process::Output {
        status: other.1,
        stdout: Vec::new(),
        stderr: other.0,
    };
    let main = outputs.pop().expect("lengths should match");
    let main = process::Output {
        status: main.1,
        stdout: Vec::new(),
        stderr: main.0,
    };

    Ok((main, other))
}

/// This uses poll, so keep `reads` small in length.
/// Keeps reading from reads until one of the fds in reads disconnects.
/// Note: Does blocking writes on our_output.
pub fn read_until_hup<T: io::Read + AsRawFd, S: io::Write>(
    children: &mut [std::process::Child],
    reads: &mut [T],
    buf_size: usize,
    mut our_output: Option<S>,
) -> io::Result<Vec<(Vec<u8>, process::ExitStatus)>> {
    let mut statuses = vec![None; children.len()];
    // Set child fds to non-blocking
    let fds = reads
        .iter()
        .map(|t| get_not_blocking_fd(t))
        .collect::<io::Result<Vec<_>>>()?;

    // Setup
    let mut pollfds = fds
        .iter()
        .map(|&fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect::<Vec<_>>();
    let mut idx_map = (0..reads.len()).collect::<Vec<_>>();
    let mut readbuf = vec![0u8; buf_size];
    let mut outputs = vec![Vec::new(); reads.len()];
    let mut failed = false;
    let timeout: libc::c_int = -1;
    let mut remove_buffer = Vec::new();
    loop {
        if pollfds.is_empty() {
            break;
        };
        let ret =
            unsafe { libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, timeout) };
        if ret == -1 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            } else {
                return Err(err);
            }
        };

        for (pollfd_idx, pollfd) in pollfds.iter().enumerate() {
            let idx = idx_map[pollfd_idx];
            if poll_readable(pollfd.revents) {
                match reads[idx].read(&mut readbuf) {
                    Ok(n) => {
                        outputs[idx].extend_from_slice(&readbuf[..n]);
                        if let Some(our_output) = our_output.as_mut() {
                            our_output.write_all(&readbuf[..n])?;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            } else if poll_ended(pollfd.revents) && statuses[idx].is_none() {
                let status = children[idx].wait()?;
                if !status.success() {
                    failed = true;
                };
                statuses[idx] = Some(status);
                remove_buffer.push(pollfd_idx);
            }
        }

        if !remove_buffer.is_empty() {
            // sort so that removing later elements do not move earlier elements
            remove_buffer.sort();
            while let Some(idx) = remove_buffer.pop() {
                pollfds.remove(idx);
                idx_map.remove(idx);
            }
        }

        if failed {
            break;
        }
    }

    if failed {
        for idx in 0..statuses.len() {
            if statuses[idx].is_none() {
                let _ = try_terminate_if_running(&mut children[idx]);
                let status = children[idx].wait()?;
                statuses[idx] = Some(status);
            }
        }
    }

    let statuses = statuses
        .into_iter()
        .map(|status| status.expect("filled in nones"));

    let output_statuses = outputs.into_iter().zip(statuses).collect::<Vec<_>>();

    Ok(output_statuses)
}

use std::{ffi, io};
use chrono::{Datelike, Timelike};

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
    if !buffer.iter().any(|&x| x == 0) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "hostname longer than 255 bytes",
        ));
    };
    // Safety: already did a null-termination check above, so safe
    let hostname_cstr = unsafe { ffi::CStr::from_ptr(buffer.as_ptr()) };
    let hostname = hostname_cstr
        .to_str()
        .map_err(|e| io::Error::other(format!("failed to obtain hostname from c string: {e}")))?;
    Ok(hostname.to_string())
}

/// Gets the date and time with a timezone offset
pub fn get_date() -> String {
    let local = chrono::Local::now();
    let year = local.year();
    let mon = local.month();
    let mday = local.day();
    let hour = local.hour();
    let min = local.minute();
    let sec = local.second();
    let tz_offset = local.offset().local_minus_utc();
    let sign = if tz_offset < 0 { "-" } else { "" }; // + is not allowed in a snapshot name
    let tz_offset = tz_offset.abs() as u32;
    let hours = tz_offset / 3600;
    let minutes = (tz_offset / 60) - (hours * 60);
    format!(
        "{year:04}-{mon:02}-{mday:02}:{hour:02}:{min:02}:{sec:02}-GMT{sign}{hours:02}:{minutes:02}"
    )
}

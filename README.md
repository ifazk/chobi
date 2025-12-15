# Chobi (ছবি) and Chithi (চিঠি)
Ports of Sanoid and Syncoid to Rust. Current focus is on Chithi (Syncoid).

# Not bug-for-bug compatible
The plan right now is to be as compatible as possible with syncoid, but I will
port features in a way that makes more sense in Rust. Particularly, Perl makes
it really easy to use regexes for things which would be very unidiomatic in
Rust. The functionality should all be there though, you should be able to to do
the same things but the command line interface might be a little different, and
some more escaping might be needed.

## Current feature deviations/shortcomings

1. Chithi: I've decided not to handle clones when replicating datasets. Part of
the reason is, I don't use clones myself. I haven't closed the door on clones,
but for now we assume `--no-clone-handling`.
2. Chithi: I've decided not to handle `--exclude-datasets` for now.
3. Chithi: I've decided not to handle `--no-rollback` for now, so syncs will
always rollback if data has been written to the recieving dataset.
4. Chithi: For hostname checks for `syncoid:sync`, the machine's hostname must
be less than 255 characters long.
5. Chithi: We only support platforms which have the `-t` option for zfs, i.e. we
don't reimpelment the fallback snapshot fetching in syncoid. This means no
solaris.
6. Chithi: We use the regex-lite crate for rexeg, and therefore do not support
unicode case insensitivity or unicode character classes like `\p{Letter}`.

# Why Rust? Why Not Go?
There are no technical or social reasons why I'm choosing Rust. Go would have
been a better option, which I also have some experience with. But I just happen
to be mainly using Rust right now, and so things will be quicker to implement on
my end.

# Rust note
It is an explicit goal to be single threaded, and use non-blocking code without
busy spinning. There should not be any thread spawning code anywhere here. It is
also an explicit goal to only rely on posix instead of needing separate special
case code for Linux and FreeBSD.

# ETA when?
Perhaps never. If I get to understand all the features of sanoid and syncoid
through this project, that's more than enough for me.

# Contributing
I am not accepting PRs or contributions to the project. The project isn't ready
for contributions. The code here is GPLv3 through, so you may fork the project
under that license if you'd like to to take the project in a different
direction, or if the updates here are too slow.

# Reporting issues
This project is not accepting any issues. I plan on opening up issues once
enough functionality is implemented.

# Plans
One of the main things I'm planning on is a deaemon for `chithi` for nightly
pushes/pulls.

I currently use systemd timers/services, but they can only have one argument (or
you have to deal with some messy escaping and parsing).  So my current work flow
looks like this:

- Have default push/pull services that push/pull to/from a single remote pool.
(e.g. syncoid-push-tank-rpool@.service syncoid-pull-tank-rpool@.service)

- For each pair of pools that need to sync, add another set of services (e.g.
syncoid-push-tank-rpool@.service syncoid-pull-tank-rpool@.service).

- For datasets that need to be renamed when transfering, add yet another set of
services (e.g.  syncoid-push-tank-rpool-dataset.service
syncoid-pull-tank-rpool-dataset.service). The last one is extra relevant for
proxmox, on the pull side `subvol-104-disk-0` is a meaningless name and likely to
conflict with subvols of other servers.

This works and I even backup all the syncoid*.{service,timer} files! And it
works reall well because systemd is very robust! But every new non-default pool
and ever new renaming gets tedius. I want a single configurating file where I
put everything down.
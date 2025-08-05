# Chobi (ছবি) and Chithi (চিঠি)
Ports of Sanoid and Syncoid to Rust. Current focus is on Chithi (Syncoid).

# Not bug-for-bug compatible
The plan right now is to be as compatible as possible with syncoid, but I will
port features in a way that makes more sense in Rust. Particularly, Perl makes
it really easy to use regexes for things which would be very unidiomatic in
Rust. The functionality should all be there though, you should be able to to do
the same things but the command line interface might be a little different, and
some more escaping might be needed.

# Why Rust?
There are no technical or social reasons why I'm choosing Rust. Go would have
been a better option, which I also have some experience with. But I just happen
to be mainly using Rust right now, and so things will be quicker to implement on
my end.

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
glcopy
======

Description
-----------

Utility to perform massive files copy over GlusterFS shares.

Compiling
---------

### Prerequisites

* make (tested with GNU Make 3.82)
* gcc (tested with 4.8.2)
* cmake (tested with 2.8.11)

### Compiling

First, initialize and update git submodules:

`git submodule init`
`git submodule update`

Then create `build` folder, chdir to it and type:

`cmake ..`

Then type `make`.

Usage
-----

The following arguments are supported:

* --from=&lt;[tcp|udp]:server:[port]:volume:path&gt; (mandatory) specifies source GlusterFS endpoint;
* --to=&lt;[tcp|udp]:server:[port]:volume:path&gt; (mandatory) specifies target GlusterFS endpoint;
* --workers=&lt;amount&gt; (optional, default "3") specifies parallel copy workers count (specifying more than 4 is dangerous due to high server CPU load!);
* --debug (optional) enables verbose output;
* --syslog (optional) logs everything to syslog instead of /dev/stderr.

There may be multiple --from and --to arguments.

Typical usage:

`glcopy --from=:data::ott:/ --to=:baikal::ott:/ --syslog`

Distribution and Contribution
-----------------------------

Distributed under terms and conditions of GNU GPL v3 (only).

The following people are involved in development:

* Oleksandr Natalenko &lt;o.natalenko@lanet.ua&gt;

Mail them any suggestions, bugreports and comments.

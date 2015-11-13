glcopy
======

Description
-----------

Utility to perform massive files copy over GlusterFS/CephFS shares.

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

* --from=&lt;glfs:[tcp|udp]:server:[port]:volume:path&gt; (mandatory) specifies source GlusterFS endpoint;
* --to=&lt;glfs:[tcp|udp]:server:[port]:volume:path&gt; (mandatory) specifies target GlusterFS endpoint;
* --from=&lt;cfs:monitors:clientid:keyring\_file:root:path&gt; (mandatory) specifies source CephFS endpoint;
* --to=&lt;cfs:monitors:clientid:keyring\_file:root:path&gt; (mandatory) specifies target CephFS endpoint;
* --workers=&lt;amount&gt; (optional, default "3") specifies parallel copy workers count (specifying more than 4 is dangerous due to high server CPU load!);
* --verbose (optional) enables verbose output;
* --debug (optional) enables debug output;
* --syslog (optional) logs everything to syslog instead of /dev/stderr.

There may be multiple --from and --to arguments.

Typical usage:

`glcopy --from=glfs::data::ott:/ --to=cfs:baikal:admin:/key:/ott:/ --verbose`

Distribution and Contribution
-----------------------------

Distributed under terms and conditions of GNU GPL v3 (only).

The following people are involved in development:

* Oleksandr Natalenko &lt;o.natalenko@lanet.ua&gt;

Mail them any suggestions, bugreports and comments.

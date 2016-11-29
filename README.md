g2c: Graphtie to Clickhouse converter
------------------------------------------

This tool can act as a proxy, taking on the input graphite line protocol and sending data to the clickhouse.

Goal is to create a tool that's easy to maintain and that won't be limiting factor in terms of graphite metric ingestion performance.

Usage
-----

TODO

Status
------
Proof of concept. Prototype. Use with caution.

It's not fully compatible with graphite line protocol yet (e.x. 'foo..bar.baz' won't be converted to 'foo.bar.baz' as it should be)

Consult https://github.com/Civil/g2c/blob/master/README.md for current plans and missing features



Acknowledgement
---------------
This program was originally developed for Booking.com.  With approval
from Booking.com, the code was generalised and published as Open Source
on github, for which the author would like to express his gratitude.

License
-------

This code is licensed under the Apache2 license.

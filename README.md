g2c: Graphtie to Clickhouse converter
------------------------------------------

This tool can act as a proxy, taking on the input graphite line protocol and sending data to the clickhouse.

Goal is to create a tool that's easy to maintain and that won't be limiting factor in terms of graphite metric ingestion performance.

Usage
-----

TODO

Clickhouse Configuration
-----

For compatibility and performance reason g2c will send metric names to a separate database. You need to create them.

In case of single-server setup:
```sql
CREATE TABLE graphite (
  Path String,
  Value Float64,
  Time UInt32,
  Date Date,
  Timestamp UInt32
) ENGINE = GraphiteMergeTree(Date, (Path, Time), 8192, 'graphite_rollup');

CREATE TABLE graphite_tree (
  Date Date,
  Level UInt32,
  Path String
) ENGINE = ReplacingMergeTree(Date, (Level, Path), 8192);
```

For a clustered clickhouse you can use following sql (on each of the servers in cluster):
```sql
CREATE TABLE graphite_local (
  Path String,
  Value Float64,
  Time UInt32,
  Date Date,
  Timestamp UInt32
) ENGINE = GraphiteMergeTree(Date, (Path, Time), 8192, 'graphite_rollup');

CREATE TABLE graphite_tree_local (
  Date Date,
  Level UInt32,
  Path String
) ENGINE = ReplacingMergeTree(Date, (Level, Path), 8192);

CREATE TABLE graphite (
  Path String,
  Value Float64,
  Time UInt32,
  Date Date,
  Timestamp UInt32
) ENGINE = Distributed(graphite, 'default', 'graphite_local', sipHash64(Path))

CREATE TABLE graphite_tree (
  Date Date,
  Level UInt32,
  Path String
) ENGINE = Distributed(graphite, 'default', 'graphite_tree_local', sipHash64(Path))
```

To achieve good results it's also good idea to add following line to your users.xml profiles/default section:

```xml
<background_pool_size>32</background_pool_size>
```

This will increase amount of background jobs that can run in parallel and will speed up ClickHouse's internal merge process. Though it will significantly increase Disk and CPU consumption.



Resource usage
-----------

Ingestion performance is 2.4M points per second on a 2xE5-2620v3, 128GB of RAM, consuming approx. 4.5 cores (CPU Usage from 400 to 500%, most of the time 450%, around 650-700% in first minute or two).

Currently it's not optimized for memory consumption (and most probably will never be) at all and for high amount of RAM. For example to process 2.4M points per second it will allocate approx. 7GB of RAM.

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

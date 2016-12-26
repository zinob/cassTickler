# Cassandra Tickler
A tool to repair an [Apache Cassandra](http://cassandra.apache.org/) table by "tickling" every record in a table at Consistency level All.

## Why?
Sometimes the built-in repair tool in Apache Cassandra does not run successfully. Or you need to repair a table, but you simply don't have the resources to allow the normal repair process of validation compactions, sstable copies, etc.

Both the pro and the con of CassTickler is that it is slow and naivé. This makes it resilient to errors and easy to monitor but might also be wasteful of both time and network transfer. Before deciding to use CassTickler you should probably consider if 
[Cassandra Range Repair](https://github.com/BrianGallew/cassandra_range_repair) might do the job.

## How does it work?
You point the tickler to your Apache Cassandra cluster and the table you want to repair. It will read (tickle) every record at a Consistency Level (CL) of ALL. In Apache Cassandra this is the highest CL offered. An interesting side effect of reading a record at CL ALL is that if any copies of the record are not consistent (or missing) the correct data will be written out to the nodes that would have the record.

nodetool repair on the other hand uses a technology known as merkel-trees to try and only transfer posts that actually differ between machines. For safety reasons this will halt (usually without an usefull error message) if any node involved in a repair is down or if there are other problems that could prevent a complete read/rewrite of the tables on both ends.

## Usage
```
tickler.py [-h] [-i [IP]] [-p [PORT]] [-t [NS]] [--keep-going] [--guess-time] [--verbose] keyspace table 
```

The trivial use-case for repairing a table on a node that is listening to connections on 127.0.0.1 is:
```
python tickler.py my_ks my_table
```

In some cases you are using cass-ticker because the cluster is degraded for one reason or another, in that case the --keep-going parameter will try to ignore replication errors and continue despite not reaching consistency all.
```
python tickler.py --keep-going my_ks my_table
```

If you also have nodetool installed locally you can also get an estimate of how long it will take to run the repair
```
python tickler.py --guess-time my_ks my_table
```

If the node does not listen for connections on 127.0.0.1 or if the repair is being run on a remote node the --ip and --port options can be specified
```
python tickler.py --ip 213.239.211.201 --port 9042 my_ks my_table
```

## Getting started
The Tickler requires Python >2.6 and >3.3 and the Python Cassandra driver to be [installed].
follow the instructions found [here](http://datastax.github.io/python-driver/installation.html) to get it installed.

Once installed, copy tickler.py to a machine that can access your Apache Cassandra cluster...and start repairing.

the current reduce implementation is wrong, map is good.

* we take M input files, and run M map functions. The partitioning function
produces R partitions on each machine's local disk.

For reduce, we are currently running one reduce function invocation
per Reduce Task Partition, meaning we end up with a sum per partition, rather
than a sum over all the partitions. This is because we havn't got a shuffle
step where all instances of a key are grouped in a single reduce worker.

We can fix this in the reduce phase by sending Reduce Tasks (numbered from 1 to
R) - to reduce workers rather than raw reduce partition filepaths.

e.g 

master --- do reduce1 --> worker <-- fetches all reduce1 partitions from other
workers, sorts the data to group together values of each key, and then passes
the result to the user Reduce function.


* we can forward all the local disk locations from the master to workers
* reduce workers can find all locations with their reduce task number and read


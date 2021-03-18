master --- do reduce1 --> worker <-- fetches all reduce1 partitions from other
workers, sorts the data to group together values of each key, and then passes
the result to the user Reduce function.


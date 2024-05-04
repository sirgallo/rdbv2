# WAL

*NOTE*

To optimize this, it may be a good design decision to separate this out into the write ahead log and then the snapshot/stats buckets in a separate db file. This way, the WAL will prioritize new log entry writes over any other write.

The upside to the current implementation is that transactions are atomic in bolt db, so all write operations included in one transaction will be considered 1 write. 
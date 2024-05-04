# Pool


## Overview

The connection pool is a mechanism to essentially recycle grpc connections once they have been made so that the connection does not need to be remade, which reduces overhead. Connections that are made are kept alive until needed.


## Sources

[Connection Pool](../internal/pool/Pool.go)
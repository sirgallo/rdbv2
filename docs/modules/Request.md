# Request


## Overview

In [Raft Consensus](https://raft.github.io/raft.pdf), the client is what interacts with the cluster and sends it operations to perform on the state machine. 


## Implementation 

This implementation integrates the Client HTTP module directly into the raft node. This was done to reduce the complexity of the overall system by limiting the total amount of systems needed. The downside to the approach is that if the client module fails, the raft node will also fail as both are coupled. There are upsides and downsides to both appraoches, but for this implementation everything is packaged together to make reasoning about the system easier.

In use, the cluster is designed to be run behind a load balancer or proxy, where requests are then sent to any node in the cluster. The node, depending on whether or not it is a follower or leader, will then determine whether or not to relay the request to the leader node or process the incoming requests.

If the node receiving the request is a follower, it will perform a redirect to the leader. Otherwise, it will process the incoming request.


## Sources

[Client](../internal/request/Service.go)
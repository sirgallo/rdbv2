# rdbv2

### a fault tolerant, distributed key-value store implemented in go


## overview

`rdbv2` is a replicated database, building off of my original implementation but updating core modules from observations made in `v1`. `rdbv2` utilizes raft consensus as the underlying distributed consensus algorithm and a simple key-value store implemented using `etcd/bbolt`.

This implementation of raft consensus aims to be readable and understandable, as well as performant.

The Raft Service is separated into modules, all of which are meant to be able to operate separately (for testing):

1. Campaign
2. Replication
3. Snapshot
4. Request

To learn more about each, check out:

[Campaign](./docs/modules/Campaign.md) 

[Replication](./docs/modules/Replication.md)

[Snapshot](./docs/modules/Snapshot.md)

[Request](./docs/modules/Request.md)


The protocol buffer schemas are as follows:

  1. RequestVoteRPC
  2. AppendEntryRPC
  3. SnapshotRPC
  
All protocol buffer schemas can be found under [api](./api).

For more information regarding the replicated log and state machine, check out:

[WAL](./docs/state/WAL.md)

[State](./docs/state/State.md)


## deployment

This implementation includes `docker-compose` configuration for running a cluster locally. By default file 5 rdb nodes are launched with a forward facing `haproxy` instance as the loadbalancer to send requests to.

### setup

Ensure `docker engine` and `docker compose` are installed on your system (for `macos`, this involves installing `docker desktop`). [Click Here](https://www.docker.com/products/docker-desktop/) to download the latest version of `docker desktop`.

The basic implementation to run the cluster and the associated `docker` resources are located under [cmd](./cmd)

Once `docker desktop` is installed, run the following to update your hostname to localhost (on `macos`):

```bash
export HOSTNAME=hostname
source ~/.zshrc
```

once sourced, restart your terminal for the changes to take effect
```bash
sudo nano /etc/hosts
```

open the file and add:
```bash
127.0.0.1 <your-hostname>
```

this binds your hostname to localhost now so you can use your hostname as the address to send commands to

## certs

Run [generateCerts](./generateCerts.sh) to guide through setting up root ca and service certs for rdb.
```bash
./generateCerts.sh
```

certs are generated under `~/rdb/certs` on the host machine.

### startup

Run the following:

```bash
chmod +x ./startupDev.sh
./startupDev.sh
```

This will build the services and then start them.

At this point, you can begin interacting with the cluster by sending it commands to perform on the state machine. 

To stop the cluster, run the `./stopDev.sh` script:
```bash
chmod +x ./stopDev.sh
./stopDev.sh
```

Stopping the cluster will bring down all of the services, but the volumes for each raft node are bound to the host machine for persistence. By default, the docker-compose file will create the volumes under your `$HOME` directory. 

For the replication db file, the path is:
```bash
$HOME/<raft-node-name>/replication/replication.db
```

For the state db file, the path is:
```bash
$HOME/<raft-node-name>/state/state.db
```

Snapshots are stored under the same directory as the statemachine db file, with the following file format:
```
state_hash.gz
```

To interact with the db files on the command line, the `bbolt` command line tool can be installed. This can be used to inspect existing buckets, stats, and information regarding the db. To install, run:

```bash
go get go.etcd.io/bbolt@latest
```

then run:
```bash
bbolt -h
```

This will give basic information regarding available commands to run against the db file.

If you want to remove the db files and snapshots, simply run:
```bash
rm -rf $HOME/<raft-node-name>
```


## Interacting with the Cluster

The statemachine expects commands with the following structure:

  1. Request

```json
{
  "action": string,
  "payload": {
    "collection": string,
    "value": string
  }
}
```

  2. Response

```json
{
  "collection": string,
  "key": string,
  "value": string
}
```

The Request is a `POST` request, which will send the request object to:
```
https://<your-host>/command
```

Here are the available commands, using `curl` to send requests to the cluster:

  1. get

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
  "action": "get",
  "payload": {
    "collection": "<your-collection>",
    "value": "<your-value>"
  }
}'
```

  2. put

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
  "action": "put",
  "payload": {
    "collection": "<your-collection>",
    "value": "<your-value>"
  }
}'
```

  3. delete

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
  "action": "delete",
  "payload": {
    "collection": "<your-collection>",
    "value": "<your-value>"
  }
}'
```

  4. drop collection

```bash
curl --location 'https://<your-host>/command' \
--header 'Content-Type: application/json' \
--data '{
  "action": "drop collection",
  "payload": {
    "collection": "<your-collection>",
    "value": "<your-value>"
  }
}'
```
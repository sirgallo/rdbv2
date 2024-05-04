# Proto


## Overview

`Protocol Buffers` are a language agnostic data serialization format, developed by `Google`. `Protocol Buffers` use a binary representation of data which make them more efficient for transferring data between systems, over something like `JSON`.


### GRPC

The Raft nodes utilize `grpc`, which is a remote procedure call framework also developed by `Google`. `Protocol Buffers` are the default data exchange format for the framework. `grpc` is well suited for building distributed systems and especially microservices.

Some key features of `grpc`:

1. Language Agnostic
2. Bidirectional Streaming
3. Strongly Typed Contracts in the form of Protocol Buffers
4. Auto Code Generation
5. Load Balancing and Service Discovery


## Building Go and Go_RPC PB files

First, install the `protoc` command. This is used to compile the `.proto` files into the language chosen for the project. `protoc` also requires plugins to compile the files into the appropriate language, and for `grpc` specific, another plugin is required. These plugins are:

1. protoc-gen-go@latest
2. protoc-gen-go-grpc@latest

These two plugins create go specific code for the proto buffer files.

To install (mac specific), run:
```bash
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

The installation on Windows or Linux distros may differ.

Once the plugins are installed, ensure that they are in your path. This can be done by editting your `.zshrc` file, or appropriate terminal (`bash`, etc.). To do so, run the following (I use `vscode` to edit, so the code command is specific to `vscode`).
```bash
code ~/.zshrc
```

In the file, add:
```bash
export GOPATH=$(go env GOPATH)
export GOBIN=$GOPATH/bin

export PATH=$PATH:/$GOBIN
```

Once added, save, and source the `.zshrc` file so that the terminal picks up the changes:
```bash
source ~/.zshrc
```

To generate the language specific code for the protobuffers, run the following:
```bash
protoc --go_out=. --go-grpc_out=. ./api/campaign.proto 
protoc --go_out=. --go-grpc_out=. ./api/replication.proto
protoc --go_out=. --go-grpc_out=. ./api/snapshot.proto 
```
## Raft Implementattion in Rust

This is a distributed consensus system based on the [Raft protocol book as spec](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf), implemented in Rust using async primitives and gRPC.

### Components

Replicated States
Election for leader takover
Cluster membership change
Log Replcation
Client interaction

### Usage

Run multiple server instances providing the internal address and client address
// TODO: Make internal service a cli maybe?

```
cargo run --bin raft-node -- <NODE_ID> <INTERNAL_ADDR> <CLIENT_ADDR>
cargo run --bin raft-node -- node1 127.0.0.1:50051 127.0.0.1:50061
cargo run --bin raft-node -- node2 127.0.0.1:50052 127.0.0.1:50062
cargo run --bin raft-node -- node3 127.0.0.1:50053 127.0.0.1:50063
```

#### Client-side Interaction

Currently we people key-value operations so you can use the cli app to interact with the client facing service. 

```
 cargo run --bin raft-client -- put mykey 42
```

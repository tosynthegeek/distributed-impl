# FROST-DKG with libp2p

A distributed threshold signature implementation using FROST (Flexible Round-Optimized Schnorr Threshold signatures) with libp2p networking stack for peer-to-peer communication.

## Features

### Round-based FROST DKG

- Round 1: Exchange commitments via gossipsub

- Round 2: Exchange packages via request-response over Kademlia

### libp2p Networking

- GossipSub for broadcast communication

- Request/Response + Kademlia for targeted peer-to-peer messaging

- mDNS for local peer discovery

- Threshold Cryptography

### Key shares generated securely across participants

- Supports multiple signers with configurable threshold

### Progress Indicators & Logging

- Step-by-step progress updates

- Logs and phase timing for each process

## Usage

Run multiple node instances providing the node name and port

### Terminal 1

```
cargo run -- --name alice --port 5001
```

### Terminal 2

```
cargo run -- --name node1 --port 5002
```

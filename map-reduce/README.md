# Usage

### Starting the Master Node

```bash
cargo run -- master
```

The master will:

- Listen on port 12224 for worker connections
- Load input files from ./map/input/
- Coordinate the MapReduce workflow
- Monitor worker health and reassign failed tasks

### Starting Worker Nodes

```bash
cargo run -- worker --port <PORT>
```

You can start multiple workers with different ports

```bash
cargo run -- worker --port 8001
cargo run -- worker --port 8002
cargo run -- worker --port 8003
```

Workers will:

- Connect to master at 127.0.0.1:12224
- Register for task assignments
- Execute map/reduce operations
- Report task completion

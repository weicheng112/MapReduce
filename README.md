# Distributed File System

This is a distributed file system implementation with a controller, storage nodes, and clients.

## Architecture

The system consists of three main components:

1. **Controller**: Manages metadata and coordinates storage nodes
2. **Storage Nodes**: Store and replicate file chunks
3. **Client**: Interacts with the controller and storage nodes to store and retrieve files

## Chunk Replication Strategy

### Chain/Relay Approach (Current Implementation)

The system uses a chain/relay approach for chunk replication:

1. The client sends a chunk to the first storage node
2. The first node stores the chunk and forwards it to the next node in the chain
3. Each subsequent node stores the chunk and forwards it to the next node
4. This continues until all nodes in the replication chain have received and stored the chunk

This approach reduces the load on the first storage node by distributing the forwarding responsibility across the chain. Each node only needs to forward the chunk to one other node, rather than the first node having to forward to all other nodes.

### Star Approach (Previous Implementation)

Previously, the system used a star approach:

1. The client sent a chunk to the first storage node
2. The first node stored the chunk and forwarded it to all other nodes in parallel
3. This put more load on the first node, as it had to handle all forwarding responsibilities

## Running the System

1. Start the controller:

   ```
   cd controller
   go run .
   ```

2. Start one or more storage nodes:

   ```
   cd storage
   go run . <port>
   ```

3. Use the client to store and retrieve files:
   ```
   cd client
   go run . store <filename>
   go run . retrieve <filename>
   ```

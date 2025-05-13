# Distributed File System with MapReduce

This is a distributed file system implementation with MapReduce functionality, featuring a controller, computation manager, storage nodes, and clients.

## Architecture

The system consists of four main components:

1. **Controller**: Manages metadata and coordinates storage nodes
2. **Computation Manager**: Coordinates MapReduce jobs and handles the shuffle phase
3. **Storage Nodes**: Store and replicate file chunks, execute map and reduce tasks
4. **Client**: Interacts with the controller and computation manager to store/retrieve files and submit MapReduce jobs

## File System Features

### Chunk Replication Strategy

The system uses a chain/relay approach for chunk replication:

1. The client sends a chunk to the first storage node
2. The first node stores the chunk and forwards it to the next node in the chain
3. Each subsequent node stores the chunk and forwards it to the next node
4. This continues until all nodes in the replication chain have received and stored the chunk

This approach reduces the load on the first storage node by distributing the forwarding responsibility across the chain.

## MapReduce Implementation

The system includes a complete MapReduce implementation with the following phases:

1. **Map Phase**: Storage nodes process input data chunks and generate key-value pairs
2. **Shuffle Phase**: Key-value pairs are grouped by key and sent to the appropriate reducer nodes
3. **Reduce Phase**: Reducers process grouped data and generate final output

### Shuffle Phase

The shuffle phase is a critical part of the MapReduce process:

1. After map tasks complete, storage nodes collect key-value pairs for each reducer
2. Storage nodes send these pairs to the computation manager
3. The computation manager forwards the data to the appropriate reducer nodes
4. This ensures all values for a particular key are processed by the same reducer

## Running the System

For detailed instructions on running the system, see:

- [MapReduce Testing Guide](docs/mapreduce_testing.md) - Instructions for testing MapReduce functionality on localhost or distributed environments
- [File Storage Guide](docs/file_storage_guide.md) - Instructions for storing files in the DFS
- [File Retrieval Guide](docs/file_retrieval_guide.md) - Instructions for retrieving files from the DFS

### Quick Start (Localhost)

1. Start the controller:
   ```
   ./controller/controller -port 8000
   ```

2. Start the computation manager:
   ```
   ./computation/computation -port 8080 -data computation_data -controller localhost:8000
   ```

3. Start storage nodes:
   ```
   ./storage/storage -id 8001 -data storage/data1 -controller localhost:8000 -computation localhost:8080
   ```

4. Use the client to store files and submit MapReduce jobs:
   ```
   ./client/client store sample_text.txt
   ./client/client mapreduce wordcount sample_text.txt wordcount_output.txt 3 true
   ```

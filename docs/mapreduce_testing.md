# Testing MapReduce Functionality

This guide explains how to test the MapReduce functionality in our distributed file system, including the shuffle phase. Instructions are provided for both localhost testing and distributed deployment on the Orion cluster.

## Prerequisites

1. Make sure you have Go installed on your system
2. Ensure all components are built

## Building the Components

First, build all the components:

```bash
# Build the controller
cd controller
go build

# Build the computation manager
cd ../computation
go build

# Build the storage nodes
cd ../storage
go build

# Build the client
cd ../client
go build

# Return to the root directory
cd ..
```

## Option 1: Testing on Localhost

This section provides detailed instructions for setting up and testing the MapReduce system on a single machine (localhost).

### Step 1: Start the Controller

Start the controller node:

```bash
./controller/controller -port 8000
```

The controller will start and listen on port 8000.

### Step 2: Start the Computation Manager

In a new terminal, start the computation manager:

```bash
./computation/computation -port 8080 -data computation_data -controller localhost:8000
```

The computation manager will start and listen on port 8080.

### Step 3: Start Storage Nodes

Start at least 3 storage nodes with different IDs in separate terminals:

```bash
# Start first storage node
./storage/storage -id 8001 -data storage/data1 -controller localhost:8000 -computation localhost:8080

# Start second storage node
./storage/storage -id 8002 -data storage/data2 -controller localhost:8000 -computation localhost:8080

# Start third storage node
./storage/storage -id 8003 -data storage/data3 -controller localhost:8000 -computation localhost:8080
```

### Step 4: Compile the Example MapReduce Jobs

Compile the example MapReduce jobs:

```bash
# Compile the word count example
go build -o wordcount examples/wordcount/wordcount.go

# Compile the log analyzer (optional)
go build -o loganalyzer examples/loganalyzer/loganalyzer.go
```

### Step 5: Store Input Files in the DFS

Create and store a sample text file:

```bash
# Create a sample text file
echo "This is a sample text file. It contains multiple words for testing the word count MapReduce job. This file will be processed by the MapReduce system to count the occurrences of each word." > sample_text.txt

# Store the text file
./client/client store sample_text.txt
```

### Step 6: Submit MapReduce Jobs

Submit a MapReduce job:

```bash
# Run the word count job
./client/client mapreduce wordcount sample_text.txt wordcount_output.txt 3 true
```

### Step 7: Retrieve and Examine Results

Once the job is complete, retrieve and view the output file:

```bash
# Retrieve the word count output
./client/client retrieve wordcount_output.txt wordcount_results.txt

# View the results
cat wordcount_results.txt
```

### Quick Localhost Setup Script

For convenience, you can use the following script to set up and test the MapReduce system on localhost. Save this as `setup_localhost.sh` and run it:

```bash
#!/bin/bash

# Build all components
echo "Building components..."
cd controller && go build && cd ..
cd computation && go build && cd ..
cd storage && go build && cd ..
cd client && go build && cd ..

# Compile the word count example
echo "Compiling word count example..."
go build -o wordcount examples/wordcount/wordcount.go

# Create sample text file
echo "Creating sample text file..."
echo "This is a sample text file. It contains multiple words for testing the word count MapReduce job. This file will be processed by the MapReduce system to count the occurrences of each word." > sample_text.txt

# Create data directories
echo "Creating data directories..."
mkdir -p computation_data
mkdir -p storage/data1 storage/data2 storage/data3

# Start controller (in background)
echo "Starting controller..."
./controller/controller -port 8000 > controller.log 2>&1 &
CONTROLLER_PID=$!
sleep 2

# Start computation manager (in background)
echo "Starting computation manager..."
./computation/computation -port 8080 -data computation_data -controller localhost:8000 > computation.log 2>&1 &
COMPUTATION_PID=$!
sleep 2

# Start storage nodes (in background)
echo "Starting storage nodes..."
./storage/storage -id 8001 -data storage/data1 -controller localhost:8000 -computation localhost:8080 > storage1.log 2>&1 &
STORAGE1_PID=$!
sleep 1
./storage/storage -id 8002 -data storage/data2 -controller localhost:8000 -computation localhost:8080 > storage2.log 2>&1 &
STORAGE2_PID=$!
sleep 1
./storage/storage -id 8003 -data storage/data3 -controller localhost:8000 -computation localhost:8080 > storage3.log 2>&1 &
STORAGE3_PID=$!
sleep 2

# Store the sample file
echo "Storing sample file in DFS..."
./client/client store sample_text.txt

# Run the word count job
echo "Running word count MapReduce job..."
./client/client mapreduce wordcount sample_text.txt wordcount_output.txt 3 true

# Retrieve the results
echo "Retrieving results..."
./client/client retrieve wordcount_output.txt wordcount_results.txt

# Display results
echo "Word count results:"
cat wordcount_results.txt

# Cleanup function
cleanup() {
  echo "Cleaning up..."
  kill $CONTROLLER_PID $COMPUTATION_PID $STORAGE1_PID $STORAGE2_PID $STORAGE3_PID
  echo "Done!"
}

# Register cleanup function
trap cleanup EXIT

# Keep script running until user presses Ctrl+C
echo "Press Ctrl+C to stop the MapReduce system"
wait
```

Make the script executable and run it:

```bash
chmod +x setup_localhost.sh
./setup_localhost.sh
```

This script will set up the entire MapReduce system on localhost, run a word count job, and display the results.

## Option 2: Testing on the Orion Cluster

For distributed testing on the Orion cluster, you'll need to run components on different machines.

### Step 1: Start the Controller

On the first machine (orion01):

```bash
./controller/controller -port 8000
```

### Step 2: Start the Computation Manager

On a suitable machine (e.g., orion02):

```bash
./computation/computation -port 8080 -data /bigdata/students/$(whoami)/computation_data -controller orion01:8000
```

### Step 3: Start Storage Nodes

Run storage nodes on multiple machines (orion02-orion12), each with a unique port and data directory:

```bash
# On orion02
./storage/storage -id 8001 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data1

# On orion03
./storage/storage -id 8002 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data2

# On orion04
./storage/storage -id 8003 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data3

# On orion05
./storage/storage -id 8004 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data4

# On orion06
./storage/storage -id 8005 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data5

# On orion07
./storage/storage -id 8006 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data6

# On orion08
./storage/storage -id 8007 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data7

# On orion09
./storage/storage -id 8008 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data8

# On orion10
./storage/storage -id 8009 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data9

# On orion11
./storage/storage -id 8010 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data10

# On orion12
./storage/storage -id 8011 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data11

# On orion12 (additional node)
./storage/storage -id 8012 -controller orion01:8000 -computation orion02:8080 -data /bigdata/students/$(whoami)/data12
```

Note: The `$(whoami)` command will automatically use your username. You can replace it with your actual username if needed.

### Step 4: Compile the Example MapReduce Jobs

On any machine:

```bash
# Compile the word count example
go build -o wordcount examples/wordcount/wordcount.go
```

### Step 5: Store Input Files in the DFS

On any machine:

```bash
# Create a sample text file
echo "This is a sample text file. It contains multiple words for testing the word count MapReduce job. This file will be processed by the MapReduce system to count the occurrences of each word." > sample_text.txt

# Store the text file
./client/client -controller orion01:8000 store sample_text.txt
```

### Step 6: Submit MapReduce Jobs

On any machine:

```bash
# Run the word count job
./client/client -controller orion01:8000 -computation orion02:8080 mapreduce wordcount sample_text.txt wordcount_output.txt 3 true
```

### Step 7: Retrieve and Examine Results

On any machine:

```bash
# Retrieve the word count output
./client/client -controller orion01:8000 retrieve wordcount_output.txt wordcount_results.txt

# View the results
cat wordcount_results.txt
```

## Important Notes for Distributed Deployment

1. **IP Address Configuration**: Always use the actual hostname or IP address of each machine when specifying controller and computation manager addresses. Do not use `localhost` when components are running on different machines.

2. **Network Connectivity**: Ensure all machines can communicate with each other over the specified ports (8000 for controller, 8080 for computation manager, and 8001-8012 for storage nodes).

3. **Firewall Settings**: Make sure the necessary ports are open in any firewalls between the machines.

4. **Data Directories**: Ensure the data directories specified for the computation manager (`/bigdata/students/$(whoami)/computation_data`) and storage nodes (`/bigdata/students/$(whoami)/data1` through `/bigdata/students/$(whoami)/data12`) exist and are writable.

5. **Shuffle Phase**: The system now correctly handles the shuffle phase by routing all shuffle data through the computation manager instead of directly between storage nodes and reducers.

## Troubleshooting

If you encounter any issues:

1. Check that all components (controller, computation manager, storage nodes) are running
2. Verify that the input files were successfully stored in the DFS
3. Check the logs of each component for error messages
4. Ensure the MapReduce jobs were compiled correctly
5. Make sure the storage nodes have enough disk space
6. For "unexpected response type" errors related to shuffle, ensure you're using the latest version with the shuffle phase implementation

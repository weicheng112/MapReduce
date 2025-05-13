# Testing MapReduce Functionality

This guide explains how to test the MapReduce functionality in our distributed file system.

## Prerequisites

1. Make sure you have Go installed on your system
2. Ensure all components are built

## Step 1: Start the Controller

First, start the controller node:

```bash
cd controller
go build
./controller
```

The controller should start and listen on port 8000 by default.

## Step 2: Start the Computation Manager

Next, start the computation manager:

```bash
cd computation
go build
./computation --controller localhost:8000
```

The computation manager should start and listen on port 8080 by default.

## Step 3: Start Storage Nodes

Start at least 3 storage nodes with different IDs:

```bash
cd storage
go build

# Start first storage node
./storage --id 8001 --controller localhost:8000 --computation localhost:8080 --data ./storage_data_1

# Start second storage node (in a new terminal)
./storage --id 8002 --controller localhost:8000 --computation localhost:8080 --data ./storage_data_2

# Start third storage node (in a new terminal)
./storage --id 8003 --controller localhost:8000 --computation localhost:8080 --data ./storage_data_3
```

## Step 4: Compile the Example MapReduce Jobs

Compile the example MapReduce jobs:

```bash
# Compile the word count example
go build -o wordcount examples/wordcount/wordcount.go

# Compile the log analyzer
go build -o loganalyzer examples/loganalyzer/loganalyzer.go
```

## Step 5: Store Input Files in the DFS

Use the client to store input files in the DFS:

```bash
cd client
go build

# Store a text file for word count
./client store sample_text.txt

# Store a log file for log analysis
./client store sample_log.txt
```

You can create sample files with the following commands:

```bash
# Create a sample text file
echo "This is a sample text file. It contains multiple words for testing the word count MapReduce job. This file will be processed by the MapReduce system to count the occurrences of each word." > sample_text.txt

# Create a sample log file
cat > sample_log.txt << EOL
192.168.1.1 - - [21/Apr/2023:10:32:15 +0000] "GET /index.html HTTP/1.1" 200 2326 "Mozilla/5.0" 0.023
192.168.1.2 - - [21/Apr/2023:10:32:16 +0000] "GET /images/logo.png HTTP/1.1" 200 4500 "Chrome/90.0" 0.015
192.168.1.3 - - [21/Apr/2023:10:32:17 +0000] "GET /css/style.css HTTP/1.1" 200 1200 "Firefox/88.0" 0.010
192.168.1.1 - - [21/Apr/2023:10:32:18 +0000] "GET /js/script.js HTTP/1.1" 200 3200 "Mozilla/5.0" 0.018
192.168.1.4 - - [21/Apr/2023:10:32:19 +0000] "POST /api/login HTTP/1.1" 401 150 "Chrome/90.0" 0.025
192.168.1.2 - - [21/Apr/2023:10:32:20 +0000] "GET /about.html HTTP/1.1" 200 1800 "Chrome/90.0" 0.012
192.168.1.5 - - [21/Apr/2023:10:32:21 +0000] "GET /contact.html HTTP/1.1" 200 1500 "Safari/14.0" 0.014
192.168.1.3 - - [21/Apr/2023:10:32:22 +0000] "POST /api/submit HTTP/1.1" 500 320 "Firefox/88.0" 0.035
192.168.1.1 - - [21/Apr/2023:10:32:23 +0000] "GET /products.html HTTP/1.1" 200 5200 "Mozilla/5.0" 0.028
192.168.1.6 - - [21/Apr/2023:10:32:24 +0000] "GET /services.html HTTP/1.1" 404 250 "Edge/91.0" 0.008
EOL
```

## Step 6: Submit MapReduce Jobs

Now you can submit MapReduce jobs using the client:

```bash
# Run the word count job
./client mapreduce wordcount sample_text.txt wordcount_output.txt 3 true

# Run the log analyzer job
./client mapreduce loganalyzer sample_log.txt loganalyzer_output.txt 7 true
```

The client will submit the job to the computation manager and monitor its progress. Once the job is complete, you can retrieve the output file:

```bash
# Retrieve the word count output
./client retrieve wordcount_output.txt wordcount_results.txt

# Retrieve the log analyzer output
./client retrieve loganalyzer_output.txt loganalyzer_results.txt
```

## Step 7: Examine the Results

Open the output files to see the results:

```bash
# View word count results
cat wordcount_results.txt

# View log analyzer results
cat loganalyzer_results.txt
```

The word count output should show each word and its count, while the log analyzer output should show various analytics about the log data.

## Troubleshooting

If you encounter any issues:

1. Check that all components (controller, computation manager, storage nodes) are running
2. Verify that the input files were successfully stored in the DFS
3. Check the logs of each component for error messages
4. Ensure the MapReduce jobs were compiled correctly
5. Make sure the storage nodes have enough disk space

## Testing on the Orion Cluster

To test on the Orion cluster:

1. Copy all the code to the cluster
2. Build all components
3. Start the controller on orion01
4. Start the computation manager on orion02
5. Start storage nodes on orion03 through orion12
6. Use the client on any node to submit jobs

Make sure to use the correct hostnames when specifying controller and computation manager addresses.

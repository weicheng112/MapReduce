# Log Analyzer Guide

This guide explains how to use the MapReduce Log Analyzer to analyze web server log files. The Log Analyzer processes log files to extract domain information and provides:

1. The number of unique domains (i.e., site.com)
2. Top 10 websites, based on the number of times their domain appears in the logs

## Overview

The Log Analyzer is implemented as two chained MapReduce jobs:

1. **First Job (loganalyzer_job1)**: Extracts domains from log entries and counts their occurrences
2. **Second Job (loganalyzer_job2)**: Sorts domains by their occurrence count in descending order

## Prerequisites

- A running distributed file system with MapReduce functionality
- Log files in a standard format (e.g., Apache or Nginx logs)

## Log File Format

The Log Analyzer expects log entries in a standard format similar to:

```
192.168.1.1 - - [21/Apr/2023:10:32:15 +0000] "GET /index.html HTTP/1.1" 200 2326 "Mozilla/5.0" 0.023
```

The analyzer extracts the URL from the request part (`GET /index.html HTTP/1.1`) and then extracts the domain from the URL.

## Step-by-Step Usage Guide

### Step 1: Prepare Your Log File

Create or obtain a log file with entries in the expected format. For example:

```
192.168.1.1 - - [21/Apr/2023:10:32:15 +0000] "GET https://example.com/index.html HTTP/1.1" 200 2326 "Mozilla/5.0" 0.023
192.168.1.2 - - [21/Apr/2023:10:32:16 +0000] "GET https://site1.com/about.html HTTP/1.1" 200 1234 "Mozilla/5.0" 0.015
```

Save this file as `sample_logs.txt`.

### Step 2: Store the Log File in the DFS

Store the log file in the distributed file system:

```bash
./client/client store sample_logs.txt
```

### Step 3: Run the First MapReduce Job

Run the first job to count domain occurrences:

```bash
./client/client mapreduce loganalyzer_job1 sample_logs.txt loganalyzer_job1_output.txt 3 true
```

Parameters:
- `loganalyzer_job1`: Path to the compiled first job binary
- `sample_logs.txt`: Input log file in the DFS
- `loganalyzer_job1_output.txt`: Output file for the first job
- `3`: Number of reducers
- `true`: Indicates the input file is text-based

### Step 4: Retrieve the First Job Results

Retrieve the results of the first job:

```bash
./client/client retrieve loganalyzer_job1_output.txt job1_results.txt
```

### Step 5: Store the First Job Results in the DFS

Store the first job results back in the DFS for the second job:

```bash
./client/client store job1_results.txt
```

### Step 6: Run the Second MapReduce Job

Run the second job to sort domains by count:

```bash
./client/client mapreduce loganalyzer_job2 job1_results.txt loganalyzer_job2_output.txt 1 true
```

Parameters:
- `loganalyzer_job2`: Path to the compiled second job binary
- `job1_results.txt`: Input file (output from the first job)
- `loganalyzer_job2_output.txt`: Output file for the second job
- `1`: Number of reducers (only 1 needed for sorting)
- `true`: Indicates the input file is text-based

### Step 7: Retrieve the Final Results

Retrieve the final sorted results:

```bash
./client/client retrieve loganalyzer_job2_output.txt final_results.txt
```

## Understanding the Results

The final output file contains lines in the format:

```
domain.com    count
```

For example:
```
site1.com    4
example.com  4
example.org  3
site2.com    3
```

The domains are sorted by count in descending order, with the most frequently occurring domains at the top.

## Troubleshooting

### Common Issues

1. **Invalid line format errors**: Ensure your log files follow the expected format.
2. **Sorting issues**: If domains are not properly sorted by count, you can use the provided `sort_results.go` utility:
   ```bash
   go build -o sort_results sort_results.go
   ./sort_results job1_results.txt sorted_results.txt
   ```

3. **Parse errors**: Check that URLs in your log files are properly formatted.

## Implementation Details

### First Job (loganalyzer_job1)

- **Map Phase**: Extracts domains from log entries and emits (domain, 1) pairs
- **Reduce Phase**: Aggregates the counts for each domain

### Second Job (loganalyzer_job2)

- **Map Phase**: Takes domain counts from the first job and emits (count, domain) pairs
- **Reduce Phase**: Collects and sorts domains by count in descending order

This approach allows for distributed processing of large log files, with each mapper processing a chunk of the log file and reducers aggregating the results.
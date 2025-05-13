# MapReduce Guide

This guide explains how to use the MapReduce functionality in our distributed file system.

## Overview

The MapReduce implementation extends our distributed file system to support distributed computation. It follows the classic MapReduce paradigm with three main phases:

1. **Map Phase**: Process input data and emit key-value pairs
2. **Shuffle Phase**: Group values by key and distribute to reducers
3. **Reduce Phase**: Process grouped values and produce final output

## Architecture

The MapReduce system consists of the following components:

1. **Computation Manager**: Coordinates MapReduce jobs, assigns tasks to nodes, and monitors progress
2. **Storage Nodes**: Execute map and reduce tasks on data they store locally
3. **Client**: Submits MapReduce jobs and monitors their progress

## Creating a MapReduce Job

A MapReduce job is a Go program that implements the Map and Reduce functions. The program should accept command-line arguments to specify whether it's running in map or reduce mode.

### Example: Word Count

Here's a simple word count example:

```go
package main

import (
    "bufio"
    "fmt"
    "os"
    "strings"
)

func main() {
    if len(os.Args) < 3 {
        fmt.Println("Usage: wordcount [map|reduce] [input_file] [output_files...]")
        os.Exit(1)
    }

    command := os.Args[1]
    inputFile := os.Args[2]

    switch command {
    case "map":
        runMap(inputFile)
    case "reduce":
        outputFile := os.Args[2]
        shuffleFiles := os.Args[3:]
        runReduce(outputFile, shuffleFiles)
    default:
        fmt.Printf("Unknown command: %s\n", command)
        os.Exit(1)
    }
}

// Map function: Read input file and emit (word, 1) pairs
func runMap(inputFile string) {
    file, err := os.Open(inputFile)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
        os.Exit(1)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        words := strings.Fields(line)

        for _, word := range words {
            word = strings.ToLower(strings.Trim(word, ".,!?\"':;()[]{}"))
            if word == "" {
                continue
            }

            // Determine reducer using simple hash
            reducer := hash(word) % 3 // Assuming 3 reducers

            // Emit to stdout in format: reducer_num\tkey\tvalue
            fmt.Printf("%d\t%s\t1\n", reducer, word)
        }
    }
}

// Reduce function: Count occurrences of each word
func runReduce(outputFile string, shuffleFiles []string) {
    wordCounts := make(map[string]int)

    for _, shuffleFile := range shuffleFiles {
        file, err := os.Open(shuffleFile)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error opening shuffle file: %v\n", err)
            continue
        }

        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            parts := strings.SplitN(line, "\t", 2)
            if len(parts) != 2 {
                continue
            }

            word := parts[0]
            count := 1 // Assuming value is always 1

            wordCounts[word] += count
        }
        file.Close()
    }

    out, err := os.Create(outputFile)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
        os.Exit(1)
    }
    defer out.Close()

    for word, count := range wordCounts {
        fmt.Fprintf(out, "%s\t%d\n", word, count)
    }
}

// Simple hash function
func hash(s string) uint32 {
    h := uint32(0)
    for i := 0; i < len(s); i++ {
        h = h*31 + uint32(s[i])
    }
    return h
}
```

## Submitting a MapReduce Job

To submit a MapReduce job, compile your Go program and use the client:

```bash
# Compile the job
go build -o wordcount examples/wordcount/wordcount.go

# Submit the job
./client mapreduce wordcount input.txt output.txt 3 true
```

Parameters:

- `wordcount`: Path to the compiled job binary
- `input.txt`: Input file in the DFS
- `output.txt`: Output file in the DFS
- `3`: Number of reducers (optional, default is 3)
- `true`: Whether the input file is text-based (optional, default is true)

## Job Execution Flow

1. The client submits the job to the Computation Manager
2. The Computation Manager:
   - Determines the chunks of the input file
   - Creates map tasks for each chunk
   - Selects reducer nodes
   - Schedules map tasks on nodes that have the chunks locally
3. Storage Nodes execute map tasks:
   - Read the chunk data
   - Execute the map function
   - Partition output by reducer
   - Send shuffle data to reducers
4. Storage Nodes execute reduce tasks:
   - Collect shuffle data from mappers
   - Sort and group by key
   - Execute the reduce function
   - Store output in the DFS
5. The Computation Manager monitors progress and reports completion to the client

## Monitoring Jobs

The client automatically monitors job progress after submission, displaying:

- Job ID
- Current state (PENDING, MAPPING, SHUFFLING, REDUCING, COMPLETED, FAILED)
- Map task progress (completed/total)
- Reduce task progress (completed/total)

## Datatype-Aware Partitioning

For text files, the system partitions data on line boundaries rather than exact byte counts. This ensures that each mapper processes complete lines of text, which is essential for text processing applications like word count.

## Load Balancing

The system balances the load across nodes by:

- Preferring data locality for map tasks (running tasks on nodes that have the data)
- Distributing reduce tasks based on node load (fewer active tasks)
- Limiting the number of concurrent tasks per node

## Error Handling

If a task fails, the system will:

1. Mark the task as failed
2. Log the error
3. Reschedule the task on a different node
4. If a job fails repeatedly, mark it as failed and report the error to the client

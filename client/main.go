package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"dfs/common"
	pb "dfs/proto"
)

// Client handles user interactions with the distributed file system
type Client struct {
	controllerAddr   string    // Address of the controller
	computationAddr  string    // Address of the computation manager
	defaultChunkSize int64     // Default size for file chunks
}

// NewClient creates a new client instance
func NewClient(controllerAddr, computationAddr string) *Client {
	return &Client{
		controllerAddr:   controllerAddr,
		computationAddr:  computationAddr,
		defaultChunkSize: common.DefaultChunkSize,
	}
}

// storeFile uploads a file to the distributed file system
// It breaks the file into chunks and distributes them to storage nodes
func (c *Client) storeFile(filepath string, chunkSize int64) error {
	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}

	// Get storage locations from controller
	locations, err := c.getStorageLocations(fileInfo.Name(), fileInfo.Size(), chunkSize)
	if err != nil {
		return fmt.Errorf("failed to get storage locations: %v", err)
	}

	// Split file into chunks
	chunks, err := common.SplitFile(file, chunkSize)
	if err != nil {
		return fmt.Errorf("failed to split file: %v", err)
	}

	// Store chunks in parallel
	var wg sync.WaitGroup
	errors := make(chan error, len(locations))

	for chunkNum, nodes := range locations {
		wg.Add(1)
		go func(num int, data []byte, storageNodes []string) {
			defer wg.Done()
			if err := c.storeChunk(fileInfo.Name(), num, data, storageNodes); err != nil {
				errors <- fmt.Errorf("chunk %d: %v", num, err)
			}
		}(chunkNum, chunks[chunkNum], nodes)
	}

	// Wait for all chunks to be stored
	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		if err != nil {
			return fmt.Errorf("failed to store file: %v", err)
		}
	}

	log.Printf("Successfully stored file %s", filepath)
	return nil
}

// retrieveFile downloads a file from the distributed file system
// It retrieves chunks in parallel and reassembles them
func (c *Client) retrieveFile(filename string, outputPath string) error {
	// Get chunk locations from controller
	locations, err := c.getChunkLocations(filename)
	if err != nil {
		return fmt.Errorf("failed to get chunk locations: %v", err)
	}

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outFile.Close()

	// Retrieve chunks in parallel
	var wg sync.WaitGroup
	errors := make(chan error, len(locations))
	chunks := make(map[int][]byte)
	var mu sync.Mutex

	for chunkNum, nodes := range locations {
		wg.Add(1)
		go func(num int, storageNodes []string) {
			defer wg.Done()
			data, err := c.retrieveChunk(filename, num, storageNodes)
			if err != nil {
				errors <- fmt.Errorf("chunk %d: %v", num, err)
				return
			}
			mu.Lock()
			chunks[num] = data
			mu.Unlock()
		}(chunkNum, nodes)
	}

	// Wait for all chunks to be retrieved
	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		if err != nil {
			return fmt.Errorf("failed to retrieve file: %v", err)
		}
	}

	// Write chunks in order
	for i := 0; i < len(locations); i++ {
		if _, err := outFile.Write(chunks[i]); err != nil {
			return fmt.Errorf("failed to write chunk %d: %v", i, err)
		}
	}

	log.Printf("Successfully retrieved file %s to %s", filename, outputPath)
	return nil
}

// deleteFile removes a file from the distributed file system
func (c *Client) deleteFile(filename string) error {
	// Request file deletion from controller
	if err := c.requestFileDeletion(filename); err != nil {
		return fmt.Errorf("failed to delete file: %v", err)
	}

	log.Printf("Successfully deleted file %s", filename)
	return nil
}

// listFiles gets a list of all files in the distributed file system
func (c *Client) listFiles() ([]*pb.FileInfo, error) {
	// Request file listing from controller
	files, err := c.requestFileList()
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %v", err)
	}

	return files, nil
}

// getNodeStatus gets status information about all storage nodes
func (c *Client) getNodeStatus() (*pb.NodeStatusResponse, error) {
	// Request node status from controller
	status, err := c.requestNodeStatus()
	if err != nil {
		return nil, fmt.Errorf("failed to get node status: %v", err)
	}

	return status, nil
}

// runInteractive starts an interactive command-line interface
func (c *Client) runInteractive() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\nDFS Client Commands:\n")
		fmt.Println("1. store <filepath> [chunk_size]")
		fmt.Println("2. retrieve <filename> <output_path>")
		fmt.Println("3. list")
		fmt.Println("4. delete <filename>")
		fmt.Println("5. status")
		fmt.Println("6. mapreduce <job_binary> <input_file> <output_file> [num_reducers] [is_text_file]")
		fmt.Println("7. exit")
		fmt.Print("\nEnter command: ")

		command, _ := reader.ReadString('\n')
		command = strings.TrimSpace(command)
		parts := strings.Fields(command)

		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "store":
			if len(parts) < 2 {
				fmt.Println("Usage: store <filepath> [chunk_size]")
				continue
			}
			chunkSize := c.defaultChunkSize
			if len(parts) > 2 {
				size, err := strconv.ParseInt(parts[2], 10, 64)
				if err != nil {
					fmt.Printf("Invalid chunk size: %v\n", err)
					continue
				}
				
				// Enforce minimum chunk size to prevent system overload
				const minChunkSize = 1024 * 1024 // 1MB minimum
				if size < minChunkSize {
					fmt.Printf("Warning: Chunk size %d is too small. Using minimum size of %d bytes (1MB) instead.\n",
						size, minChunkSize)
					size = minChunkSize
				}
				chunkSize = size
			}
			if err := c.storeFile(parts[1], chunkSize); err != nil {
				fmt.Printf("Error storing file: %v\n", err)
			} else {
				fmt.Println("File stored successfully")
			}

		case "retrieve":
			if len(parts) != 3 {
				fmt.Println("Usage: retrieve <filename> <output_path>")
				continue
			}
			if err := c.retrieveFile(parts[1], parts[2]); err != nil {
				fmt.Printf("Error retrieving file: %v\n", err)
			} else {
				fmt.Println("File retrieved successfully")
			}

		case "list":
			files, err := c.listFiles()
			if err != nil {
				fmt.Printf("Error listing files: %v\n", err)
				continue
			}
			fmt.Println("\nFiles in DFS:")
			fmt.Println("Name\tSize\tChunks")
			fmt.Println("----\t----\t------")
			for _, file := range files {
				fmt.Printf("%s\t%d\t%d\n", file.Filename, file.Size, file.NumChunks)
			}

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <filename>")
				continue
			}
			if err := c.deleteFile(parts[1]); err != nil {
				fmt.Printf("Error deleting file: %v\n", err)
			} else {
				fmt.Println("File deleted successfully")
			}

		case "status":
			status, err := c.getNodeStatus()
			if err != nil {
				fmt.Printf("Error getting node status: %v\n", err)
				continue
			}
			fmt.Println("\nStorage Node Status:")
			fmt.Println("Node ID\tFree Space\tRequests Handled")
			fmt.Println("-------\t----------\t---------------")
			for _, node := range status.Nodes {
				fmt.Printf("%s\t%d GB\t%d\n", 
					node.NodeId, 
					node.FreeSpace/(1024*1024*1024), 
					node.RequestsProcessed)
			}
			fmt.Printf("\nTotal Available Space: %d GB\n", status.TotalSpace/(1024*1024*1024))

		case "mapreduce":
			if len(parts) < 4 {
				fmt.Println("Usage: mapreduce <job_binary> <input_file> <output_file> [num_reducers] [is_text_file]")
				continue
			}
			
			jobBinaryPath := parts[1]
			inputFile := parts[2]
			outputFile := parts[3]
			
			// Default values
			numReducers := uint32(common.DefaultNumReducers)
			isTextFile := true
			
			// Parse optional arguments
			if len(parts) > 4 {
				var n int
				_, err := fmt.Sscanf(parts[4], "%d", &n)
				if err == nil && n > 0 {
					numReducers = uint32(n)
				}
			}
			
			if len(parts) > 5 {
				isTextFile = parts[5] == "true"
			}
			
			// Submit job
			if err := c.submitJob(jobBinaryPath, inputFile, outputFile, numReducers, isTextFile); err != nil {
				fmt.Printf("Error submitting job: %v\n", err)
			}

		case "exit":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}

func main() {
	// Parse command line arguments
	controllerAddr := flag.String("controller", "localhost:8000", "Controller address")
	computationAddr := flag.String("computation", "localhost:8080", "Computation manager address")
	
	// Check for command-line mode
	flag.Parse()
	args := flag.Args()
	
	// Create client
	client := NewClient(*controllerAddr, *computationAddr)
	
	// Check if we're running in command-line mode
	if len(args) > 0 {
		switch args[0] {
		case "store":
			if len(args) < 2 {
				fmt.Println("Usage: client store <filepath> [chunk_size]")
				os.Exit(1)
			}
			chunkSize := client.defaultChunkSize
			if len(args) > 2 {
				size, err := strconv.ParseInt(args[2], 10, 64)
				if err == nil && size > 0 {
					chunkSize = size
				}
			}
			if err := client.storeFile(args[1], chunkSize); err != nil {
				log.Fatalf("Error storing file: %v", err)
			}
			
		case "retrieve":
			if len(args) != 3 {
				fmt.Println("Usage: client retrieve <filename> <output_path>")
				os.Exit(1)
			}
			if err := client.retrieveFile(args[1], args[2]); err != nil {
				log.Fatalf("Error retrieving file: %v", err)
			}
			
		case "list":
			files, err := client.listFiles()
			if err != nil {
				log.Fatalf("Error listing files: %v", err)
			}
			fmt.Println("Files in DFS:")
			fmt.Println("Name\tSize\tChunks")
			fmt.Println("----\t----\t------")
			for _, file := range files {
				fmt.Printf("%s\t%d\t%d\n", file.Filename, file.Size, file.NumChunks)
			}
			
		case "delete":
			if len(args) != 2 {
				fmt.Println("Usage: client delete <filename>")
				os.Exit(1)
			}
			if err := client.deleteFile(args[1]); err != nil {
				log.Fatalf("Error deleting file: %v", err)
			}
			
		case "status":
			status, err := client.getNodeStatus()
			if err != nil {
				log.Fatalf("Error getting node status: %v", err)
			}
			fmt.Println("Storage Node Status:")
			fmt.Println("Node ID\tFree Space\tRequests Handled")
			fmt.Println("-------\t----------\t---------------")
			for _, node := range status.Nodes {
				fmt.Printf("%s\t%d GB\t%d\n",
					node.NodeId,
					node.FreeSpace/(1024*1024*1024),
					node.RequestsProcessed)
			}
			fmt.Printf("Total Available Space: %d GB\n", status.TotalSpace/(1024*1024*1024))
			
		case "mapreduce":
			client.handleMapReduceCommand()
			
		default:
			fmt.Printf("Unknown command: %s\n", args[0])
			os.Exit(1)
		}
	} else {
		// Run in interactive mode
		client.runInteractive()
	}
}
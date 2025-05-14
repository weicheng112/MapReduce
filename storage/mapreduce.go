package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// MapReduceHandler handles MapReduce tasks
type MapReduceHandler struct {
	node            *StorageNode
	activeTasks     map[string]*ActiveTask // Map of task ID to active task
	activeTaskMutex sync.RWMutex
	tempDir         string // Directory for temporary files
}

// ActiveTask represents an active MapReduce task
type ActiveTask struct {
	ID             string
	JobID          string
	Type           string // "map" or "reduce"
	ChunkNumber    uint32 // For map tasks
	ReducerNumber  uint32 // For reduce tasks
	JobBinary      []byte
	BinaryPath     string
	OutputFiles    []string
	Reducers       []*pb.ReducerInfo // For map tasks
}

// MapperInterface defines the interface for mapper plugins
type MapperInterface interface {
	Map(key, value string) []KeyValuePair
}

// ReducerInterface defines the interface for reducer plugins
type ReducerInterface interface {
	Reduce(key string, values []string) string
}

// KeyValuePair represents a key-value pair
type KeyValuePair struct {
	Key   string
	Value string
}

// NewMapReduceHandler creates a new MapReduce handler
func NewMapReduceHandler(node *StorageNode) *MapReduceHandler {
	tempDir := filepath.Join(node.dataDir, "mapreduce_temp")
	os.MkdirAll(tempDir, 0755)

	return &MapReduceHandler{
		node:        node,
		activeTasks: make(map[string]*ActiveTask),
		tempDir:     tempDir,
	}
}

// HandleMapTask handles a map task request
func (h *MapReduceHandler) HandleMapTask(data []byte) ([]byte, error) {
	// Parse request
	request := &pb.MapTaskRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal map task request: %v", err)
	}

	log.Printf("Received map task request for job: %s, task: %s, chunk: %d", 
		request.JobId, request.TaskId, request.ChunkNumber)

	// Create active task
	task := &ActiveTask{
		ID:          request.TaskId,
		JobID:       request.JobId,
		Type:        "map",
		ChunkNumber: request.ChunkNumber,
		JobBinary:   request.JobBinary,
		Reducers:    request.Reducers,
	}

	// Save job binary to temporary file with a unique name for each task
	binaryPath := filepath.Join(h.tempDir, fmt.Sprintf("job_%s_task_%s.bin", request.JobId, request.TaskId))
	if err := ioutil.WriteFile(binaryPath, request.JobBinary, 0755); err != nil {
		return nil, fmt.Errorf("failed to write job binary: %v", err)
	}
	task.BinaryPath = binaryPath

	// Add task to active tasks
	h.activeTaskMutex.Lock()
	h.activeTasks[request.TaskId] = task
	h.activeTaskMutex.Unlock()

	// Execute map task in a goroutine
	go h.executeMapTask(task, request.Filename, request.ChunkNumber)

	// Return success response
	response := &pb.MapTaskResponse{
		JobId:   request.JobId,
		TaskId:  request.TaskId,
		Success: true,
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// Maximum number of concurrent reduce tasks
const maxConcurrentReduceTasks = 1

// HandleReduceTask handles a reduce task request
func (h *MapReduceHandler) HandleReduceTask(data []byte) ([]byte, error) {
	// Parse request
	request := &pb.ReduceTaskRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal reduce task request: %v", err)
	}

	log.Printf("Received reduce task request for job: %s, task: %s, reducer: %d",
		request.JobId, request.TaskId, request.ReducerNumber)

	// Check if we already have too many active reduce tasks
	h.activeTaskMutex.RLock()
	activeReduceTasks := 0
	for _, t := range h.activeTasks {
		if t.Type == "reduce" {
			activeReduceTasks++
		}
	}
	h.activeTaskMutex.RUnlock()

	if activeReduceTasks >= maxConcurrentReduceTasks {
		log.Printf("Too many active reduce tasks (%d), rejecting task: %s (reducer: %d)",
			activeReduceTasks, request.TaskId, request.ReducerNumber)
		
		// Create failure response with a special error message that indicates this is a temporary rejection
		response := &pb.ReduceTaskResponse{
			JobId:   request.JobId,
			TaskId:  request.TaskId,
			Success: false,
			Error:   fmt.Sprintf("TEMPORARY_BUSY: Node busy with %d reduce tasks, retry later", activeReduceTasks),
		}
		
		// Serialize response
		responseData, err := proto.Marshal(response)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal response: %v", err)
		}
		
		// Add a small delay before responding to prevent rapid retries
		time.Sleep(100 * time.Millisecond)
		
		return responseData, nil
	}

	// Create active task
	task := &ActiveTask{
		ID:            request.TaskId,
		JobID:         request.JobId,
		Type:          "reduce",
		ReducerNumber: request.ReducerNumber,
		JobBinary:     request.JobBinary,
	}

	// Save job binary to temporary file with a unique name for each task
	binaryPath := filepath.Join(h.tempDir, fmt.Sprintf("job_%s_task_%s.bin", request.JobId, request.TaskId))
	if err := ioutil.WriteFile(binaryPath, request.JobBinary, 0755); err != nil {
		return nil, fmt.Errorf("failed to write job binary: %v", err)
	}
	task.BinaryPath = binaryPath

	// Add task to active tasks
	h.activeTaskMutex.Lock()
	h.activeTasks[request.TaskId] = task
	h.activeTaskMutex.Unlock()

	// Execute reduce task in a goroutine
	go h.executeReduceTask(task, request.OutputFile)

	// Return success response
	response := &pb.ReduceTaskResponse{
		JobId:   request.JobId,
		TaskId:  request.TaskId,
		Success: true,
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// HandleShuffle handles a shuffle request
func (h *MapReduceHandler) HandleShuffle(data []byte) ([]byte, error) {
	// Parse request
	request := &pb.ShuffleRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal shuffle request: %v", err)
	}

	log.Printf("Received shuffle request for job: %s, mapper: %s, reducer: %d", 
		request.JobId, request.MapperId, request.ReducerNumber)

	// Create directory for shuffle data
	shuffleDir := filepath.Join(h.tempDir, fmt.Sprintf("job_%s_reducer_%d", request.JobId, request.ReducerNumber))
	if err := os.MkdirAll(shuffleDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shuffle directory: %v", err)
	}

	// Write shuffle data to file
	shuffleFile := filepath.Join(shuffleDir, fmt.Sprintf("mapper_%s.txt", request.MapperId))
	file, err := os.Create(shuffleFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create shuffle file: %v", err)
	}
	defer file.Close()

	// Write key-value pairs to file
	for _, pair := range request.KeyValuePairs {
		if _, err := fmt.Fprintf(file, "%s\t%s\n", pair.Key, pair.Value); err != nil {
			return nil, fmt.Errorf("failed to write to shuffle file: %v", err)
		}
	}

	// Return success response
	response := &pb.ShuffleResponse{
		JobId:   request.JobId,
		Success: true,
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	return responseData, nil
}

// executeMapTask executes a map task
func (h *MapReduceHandler) executeMapTask(task *ActiveTask, filename string, chunkNumber uint32) {
	log.Printf("Executing map task: %s for job: %s", task.ID, task.JobID)

	// Create response
	response := &pb.MapTaskResponse{
		JobId:  task.JobID,
		TaskId: task.ID,
	}

	// Get chunk data
	chunkData, err := h.node.retrieveChunk(filename, int(chunkNumber))
	if err != nil {
		log.Printf("Failed to retrieve chunk: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to retrieve chunk: %v", err)
		h.sendMapTaskResponse(response)
		return
	}

	// Create temporary directory for this task
	taskDir := filepath.Join(h.tempDir, fmt.Sprintf("job_%s_task_%s", task.JobID, task.ID))
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		log.Printf("Failed to create task directory: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to create task directory: %v", err)
		h.sendMapTaskResponse(response)
		return
	}

	// Write chunk data to file
	chunkFile := filepath.Join(taskDir, "input.txt")
	if err := ioutil.WriteFile(chunkFile, chunkData, 0644); err != nil {
		log.Printf("Failed to write chunk data: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to write chunk data: %v", err)
		h.sendMapTaskResponse(response)
		return
	}

	// Create output files for each reducer
	outputFiles := make([]string, len(task.Reducers))
	for i, reducer := range task.Reducers {
		outputFile := filepath.Join(taskDir, fmt.Sprintf("output_%d.txt", reducer.ReducerNumber))
		outputFiles[i] = outputFile
		// Create empty file
		file, err := os.Create(outputFile)
		if err != nil {
			log.Printf("Failed to create output file: %v", err)
			response.Success = false
			response.Error = fmt.Sprintf("Failed to create output file: %v", err)
			h.sendMapTaskResponse(response)
			return
		}
		file.Close()
	}
	task.OutputFiles = outputFiles

	// Execute map function
	if err := h.executeMapFunction(task, chunkFile); err != nil {
		log.Printf("Failed to execute map function: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to execute map function: %v", err)
		h.sendMapTaskResponse(response)
		return
	}

	// Send shuffle data to reducers
	if err := h.sendShuffleData(task); err != nil {
		log.Printf("Failed to send shuffle data: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to send shuffle data: %v", err)
		h.sendMapTaskResponse(response)
		return
	}

	// Update response
	response.Success = true
	response.IntermediateFiles = task.OutputFiles

	// Send response to computation manager
	h.sendMapTaskResponse(response)

	log.Printf("Map task completed successfully: %s", task.ID)

	// Remove task from active tasks
	h.activeTaskMutex.Lock()
	delete(h.activeTasks, task.ID)
	h.activeTaskMutex.Unlock()
}

// executeReduceTask executes a reduce task
func (h *MapReduceHandler) executeReduceTask(task *ActiveTask, outputFile string) {
	log.Printf("Executing reduce task: %s for job: %s", task.ID, task.JobID)

	// Create response
	response := &pb.ReduceTaskResponse{
		JobId:  task.JobID,
		TaskId: task.ID,
	}
	
	// Set up error handling and recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in executeReduceTask: %v", r)
			response.Success = false
			response.Error = fmt.Sprintf("Task panic: %v", r)
			h.sendReduceTaskResponse(response)
		}
	}()

	// Create temporary directory for this task
	taskDir := filepath.Join(h.tempDir, fmt.Sprintf("job_%s_reducer_%d", task.JobID, task.ReducerNumber))
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		log.Printf("Failed to create task directory: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to create task directory: %v", err)
		h.sendReduceTaskResponse(response)
		return
	}

	// Get all shuffle files
	shuffleFiles, err := filepath.Glob(filepath.Join(taskDir, "mapper_*.txt"))
	if err != nil {
		log.Printf("Failed to get shuffle files: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to get shuffle files: %v", err)
		h.sendReduceTaskResponse(response)
		return
	}

	// Check if there are any shuffle files
	if len(shuffleFiles) == 0 {
		log.Printf("No shuffle files found for reducer %d in directory %s", task.ReducerNumber, taskDir)
		// List all files in the directory to debug
		files, _ := ioutil.ReadDir(taskDir)
		for _, file := range files {
			log.Printf("File in reducer directory: %s", file.Name())
		}
		response.Success = false
		response.Error = fmt.Sprintf("No shuffle files found for reducer %d", task.ReducerNumber)
		h.sendReduceTaskResponse(response)
		return
	}
	
	log.Printf("Found %d shuffle files for reducer %d: %v", len(shuffleFiles), task.ReducerNumber, shuffleFiles)

	// Create output file in a shared directory that the computation manager can access
	// Use a directory structure that's predictable and accessible
	sharedDir := filepath.Join(h.node.dataDir, "shared_outputs")
	jobDir := filepath.Join(sharedDir, fmt.Sprintf("job_%s", task.JobID))
	outputDir := filepath.Join(jobDir, fmt.Sprintf("reducer_%d", task.ReducerNumber))
	
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Printf("Failed to create output directory: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to create output directory: %v", err)
		h.sendReduceTaskResponse(response)
		return
	}
	
	outputPath := filepath.Join(outputDir, "output.txt")
	log.Printf("Creating output file at %s", outputPath)
	file, err := os.Create(outputPath)
	if err != nil {
		log.Printf("Failed to create output file: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to create output file: %v", err)
		h.sendReduceTaskResponse(response)
		return
	}
	file.Close()
	task.OutputFiles = []string{outputPath}

	// Execute reduce function
	if err := h.executeReduceFunction(task, shuffleFiles, outputPath); err != nil {
		log.Printf("Failed to execute reduce function: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to execute reduce function: %v", err)
		h.sendReduceTaskResponse(response)
		return
	}

	// Store the output file in the DFS with a temporary name
	tempDFSFilename := fmt.Sprintf("__temp_reducer_%s_%d.txt", task.JobID, task.ReducerNumber)
	
	// Store the output file in the DFS
	if err := h.storeOutputFile(outputPath, tempDFSFilename); err != nil {
		log.Printf("Failed to store output file in DFS: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to store output file in DFS: %v", err)
		h.sendReduceTaskResponse(response)
		return
	}
	
	log.Printf("Stored output file in DFS as %s", tempDFSFilename)
	
	// Update response with the DFS file path
	response.Success = true
	response.OutputFile = tempDFSFilename  // Send the DFS file path

	// Send response to computation manager
	h.sendReduceTaskResponse(response)

	log.Printf("Reduce task completed successfully: %s", task.ID)

	// Remove task from active tasks
	h.activeTaskMutex.Lock()
	delete(h.activeTasks, task.ID)
	h.activeTaskMutex.Unlock()
}

// executeMapFunction executes the map function on the input file
func (h *MapReduceHandler) executeMapFunction(task *ActiveTask, inputFile string) error {
	// For now, we'll use a simple approach: execute the job binary as a separate process
	// In a more sophisticated implementation, we could use Go plugins

	log.Printf("Executing map function for task %s with binary %s", task.ID, task.BinaryPath)
	log.Printf("Input file: %s", inputFile)

	// Check if binary exists
	if _, err := os.Stat(task.BinaryPath); os.IsNotExist(err) {
		log.Printf("ERROR: Job binary does not exist at %s", task.BinaryPath)
		return fmt.Errorf("job binary does not exist: %v", err)
	}
	
	// Copy the job binary to a task-specific location to avoid "text file busy" errors
	taskDir := filepath.Dir(inputFile)
	taskSpecificBinary := filepath.Join(taskDir, fmt.Sprintf("job_binary_%s.bin", task.ID))
	log.Printf("Copying job binary to task-specific location: %s", taskSpecificBinary)
	
	// Read the original binary
	binaryData, err := ioutil.ReadFile(task.BinaryPath)
	if err != nil {
		log.Printf("ERROR: Failed to read job binary: %v", err)
		return fmt.Errorf("failed to read job binary: %v", err)
	}
	
	// Write to task-specific location
	if err := ioutil.WriteFile(taskSpecificBinary, binaryData, 0755); err != nil {
		log.Printf("ERROR: Failed to write task-specific binary: %v", err)
		return fmt.Errorf("failed to write task-specific binary: %v", err)
	}

	// Add a delay to ensure the file is fully written and the file system has updated
	log.Printf("Waiting for file system to stabilize...")
	time.Sleep(200 * time.Millisecond)

	// Verify the binary exists and is executable
	if err := verifyExecutable(taskSpecificBinary); err != nil {
		log.Printf("ERROR: Binary is not executable: %v", err)
		return fmt.Errorf("binary is not executable: %v", err)
	}

	// Create command using the task-specific binary
	cmd := exec.Command(taskSpecificBinary, "map", inputFile)
	
	// Create pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		// Clean up task-specific binary
		os.Remove(taskSpecificBinary)
		return fmt.Errorf("failed to create stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		// Clean up task-specific binary
		os.Remove(taskSpecificBinary)
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	// Start command
	log.Printf("Starting map command")
	if err := cmd.Start(); err != nil {
		// Clean up task-specific binary
		os.Remove(taskSpecificBinary)
		return fmt.Errorf("failed to start command: %v", err)
	}

	// Read stdout
	outputFiles := make(map[uint32]*os.File)
	for _, reducer := range task.Reducers {
		file, err := os.OpenFile(task.OutputFiles[reducer.ReducerNumber], os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open output file: %v", err)
		}
		defer file.Close()
		outputFiles[reducer.ReducerNumber] = file
	}

	// Process output
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 {
			log.Printf("Invalid output line: %s", line)
			continue
		}

		// Parse reducer number
		reducerNum, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			log.Printf("Invalid reducer number: %s", parts[0])
			continue
		}

		// Write to output file
		file, exists := outputFiles[uint32(reducerNum)]
		if !exists {
			log.Printf("Reducer not found: %d", reducerNum)
			continue
		}

		if _, err := fmt.Fprintf(file, "%s\t%s\n", parts[1], parts[2]); err != nil {
			log.Printf("Failed to write to output file: %v", err)
			continue
		}
	}

	// Read stderr
	stderrBytes, err := ioutil.ReadAll(stderr)
	if err != nil {
		return fmt.Errorf("failed to read stderr: %v", err)
	}
	if len(stderrBytes) > 0 {
		log.Printf("Map task stderr: %s", string(stderrBytes))
	}

	// Wait for command to finish
	log.Printf("Waiting for map command to finish")
	if err := cmd.Wait(); err != nil {
		log.Printf("ERROR: Command failed: %v", err)
		
		// Clean up task-specific binary
		os.Remove(taskSpecificBinary)
		
		return fmt.Errorf("command failed: %v", err)
	}

	// Clean up task-specific binary
	if err := os.Remove(taskSpecificBinary); err != nil {
		log.Printf("Warning: Failed to remove task-specific binary %s: %v", taskSpecificBinary, err)
	} else {
		log.Printf("Removed task-specific binary: %s", taskSpecificBinary)
	}

	log.Printf("Map function executed successfully")
	return nil
}

// executeReduceFunction executes the reduce function on the shuffle files
func (h *MapReduceHandler) executeReduceFunction(task *ActiveTask, shuffleFiles []string, outputFile string) error {
	// For now, we'll use a simple approach: execute the job binary as a separate process
	// In a more sophisticated implementation, we could use Go plugins

	log.Printf("Executing reduce function for task %s with binary %s", task.ID, task.BinaryPath)
	log.Printf("Output file: %s", outputFile)
	log.Printf("Shuffle files (%d): %v", len(shuffleFiles), shuffleFiles)

	// Check if binary exists
	if _, err := os.Stat(task.BinaryPath); os.IsNotExist(err) {
		log.Printf("ERROR: Job binary does not exist at %s", task.BinaryPath)
		return fmt.Errorf("job binary does not exist: %v", err)
	}
	
	// Copy the job binary to a task-specific location to avoid "text file busy" errors
	taskSpecificBinary := filepath.Join(filepath.Dir(outputFile), fmt.Sprintf("job_binary_%s.bin", task.ID))
	log.Printf("Copying job binary to task-specific location: %s", taskSpecificBinary)
	
	// Read the original binary
	binaryData, err := ioutil.ReadFile(task.BinaryPath)
	if err != nil {
		log.Printf("ERROR: Failed to read job binary: %v", err)
		return fmt.Errorf("failed to read job binary: %v", err)
	}
	
	// Write to task-specific location
	if err := ioutil.WriteFile(taskSpecificBinary, binaryData, 0755); err != nil {
		log.Printf("ERROR: Failed to write task-specific binary: %v", err)
		return fmt.Errorf("failed to write task-specific binary: %v", err)
	}

	// Add a longer delay to ensure the file is fully written and the file system has updated
	log.Printf("Waiting for file system to stabilize...")
	time.Sleep(500 * time.Millisecond)

	// Verify the binary exists and is executable
	if _, err := os.Stat(taskSpecificBinary); os.IsNotExist(err) {
		log.Printf("ERROR: Binary file does not exist after writing: %s", taskSpecificBinary)
		return fmt.Errorf("binary file does not exist after writing: %s", taskSpecificBinary)
	}

	// Verify the binary is executable
	if err := verifyExecutable(taskSpecificBinary); err != nil {
		log.Printf("ERROR: Binary is not executable: %v", err)
		return fmt.Errorf("binary is not executable: %v", err)
	}

	// Double-check file size
	fileInfo, err := os.Stat(taskSpecificBinary)
	if err != nil {
		log.Printf("ERROR: Failed to stat binary file: %v", err)
		return fmt.Errorf("failed to stat binary file: %v", err)
	}
	
	if fileInfo.Size() == 0 {
		log.Printf("ERROR: Binary file is empty: %s", taskSpecificBinary)
		return fmt.Errorf("binary file is empty: %s", taskSpecificBinary)
	}
	
	log.Printf("Binary file verified: %s (size: %d bytes, mode: %s)",
		taskSpecificBinary, fileInfo.Size(), fileInfo.Mode().String())

	// Create command using the task-specific binary
	args := append([]string{"reduce", outputFile}, shuffleFiles...)
	log.Printf("Command: %s %v", taskSpecificBinary, args)
	cmd := exec.Command(taskSpecificBinary, args...)
	
	// Create pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("ERROR: Failed to create stdout pipe: %v", err)
		return fmt.Errorf("failed to create stdout pipe: %v", err)
	}
	
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("ERROR: Failed to create stderr pipe: %v", err)
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	// Start command with retries
	log.Printf("Starting reduce command")
	maxStartRetries := 3
	var startErr error
	
	for i := 0; i < maxStartRetries; i++ {
		startErr = cmd.Start()
		if startErr == nil {
			// Command started successfully
			break
		}
		
		log.Printf("ERROR: Failed to start command (attempt %d/%d): %v", i+1, maxStartRetries, startErr)
		
		// If this is the last attempt, return the error
		if i == maxStartRetries-1 {
			// Clean up task-specific binary
			os.Remove(taskSpecificBinary)
			return fmt.Errorf("failed to start command after %d attempts: %v", maxStartRetries, startErr)
		}
		
		// Wait before retrying
		retryDelay := time.Duration(i+1) * 500 * time.Millisecond
		log.Printf("Retrying command start in %v...", retryDelay)
		time.Sleep(retryDelay)
		
		// Create a new command for the retry
		cmd = exec.Command(taskSpecificBinary, args...)
	}
	
	// Read stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			log.Printf("Reduce stdout: %s", scanner.Text())
		}
	}()

	// Read stderr
	stderrBytes, err := ioutil.ReadAll(stderr)
	if err != nil {
		log.Printf("ERROR: Failed to read stderr: %v", err)
		return fmt.Errorf("failed to read stderr: %v", err)
	}
	if len(stderrBytes) > 0 {
		log.Printf("Reduce task stderr: %s", string(stderrBytes))
	}

	// Wait for command to finish
	log.Printf("Waiting for reduce command to finish")
	if err := cmd.Wait(); err != nil {
		log.Printf("ERROR: Command failed: %v", err)
		
		// Clean up task-specific binary even on failure
		os.Remove(taskSpecificBinary)
		
		return fmt.Errorf("command failed: %v", err)
	}

	// Check if output file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		log.Printf("ERROR: Output file was not created at %s", outputFile)
		
		// Clean up task-specific binary
		os.Remove(taskSpecificBinary)
		
		return fmt.Errorf("output file was not created: %v", err)
	} else if err != nil {
		log.Printf("ERROR: Failed to stat output file: %v", err)
		
		// Clean up task-specific binary
		os.Remove(taskSpecificBinary)
		
		return fmt.Errorf("failed to stat output file: %v", err)
	} else {
		fileInfo, _ := os.Stat(outputFile)
		log.Printf("Output file created successfully: %s (size: %d bytes)", outputFile, fileInfo.Size())
	}

	// Clean up task-specific binary
	if err := os.Remove(taskSpecificBinary); err != nil {
		log.Printf("Warning: Failed to remove task-specific binary %s: %v", taskSpecificBinary, err)
	} else {
		log.Printf("Removed task-specific binary: %s", taskSpecificBinary)
	}

	log.Printf("Reduce function executed successfully")
	return nil
}

// sendShuffleData sends shuffle data to reducers
func (h *MapReduceHandler) sendShuffleData(task *ActiveTask) error {
	// For each reducer
	for i, reducer := range task.Reducers {
		// Read output file
		outputFile := task.OutputFiles[i]
		data, err := ioutil.ReadFile(outputFile)
		if err != nil {
			return fmt.Errorf("failed to read output file: %v", err)
		}

		// Parse key-value pairs
		pairs := make([]*pb.KeyValuePair, 0)
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) != 2 {
				log.Printf("Invalid line in output file: %s", line)
				continue
			}

			pairs = append(pairs, &pb.KeyValuePair{
				Key:   []byte(parts[0]),
				Value: []byte(parts[1]),
			})
		}

		// Create shuffle request
		request := &pb.ShuffleRequest{
			JobId:         task.JobID,
			MapperId:      task.ID,
			ReducerNumber: reducer.ReducerNumber,
			KeyValuePairs: pairs,
		}

		// Send shuffle request to computation manager instead of directly to reducer
		if err := h.sendShuffleRequest(h.node.computationAddr, request); err != nil {
			return fmt.Errorf("failed to send shuffle request: %v", err)
		}
	}

	return nil
}

// sendShuffleRequest sends a shuffle request to the computation manager
func (h *MapReduceHandler) sendShuffleRequest(computationAddr string, request *pb.ShuffleRequest) error {
	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Try to send the request with retries
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		// Connect to computation manager
		conn, err := net.Dial("tcp", computationAddr)
		if err != nil {
			log.Printf("Failed to connect to computation manager (attempt %d/%d): %v", i+1, maxRetries, err)
			if i == maxRetries-1 {
				return fmt.Errorf("failed to connect to computation manager after %d attempts: %v", maxRetries, err)
			}
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}
		
		// Send request
		err = common.WriteMessage(conn, common.MsgTypeShuffle, requestData)
		if err != nil {
			conn.Close()
			log.Printf("Failed to send shuffle request (attempt %d/%d): %v", i+1, maxRetries, err)
			if i == maxRetries-1 {
				return fmt.Errorf("failed to send shuffle request after %d attempts: %v", maxRetries, err)
			}
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}

		// Read response
		msgType, responseData, err := common.ReadMessage(conn)
		conn.Close()
		
		if err != nil {
			// Check if it's an EOF error
			if strings.Contains(err.Error(), "EOF") {
				log.Printf("EOF error when reading response (attempt %d/%d): %v", i+1, maxRetries, err)
				log.Printf("This is likely due to the storage node closing the connection")
				
				// For EOF errors, we can consider the message as sent if this is a retry
				if i >= 1 {
					log.Printf("Considering request as successful after EOF on retry %d", i+1)
					return nil
				}
			} else {
				log.Printf("Failed to read shuffle response (attempt %d/%d): %v", i+1, maxRetries, err)
			}
			
			if i == maxRetries-1 {
				return fmt.Errorf("failed to read shuffle response after %d attempts: %v", maxRetries, err)
			}
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}

		if msgType != common.MsgTypeShuffleResponse {
			log.Printf("Unexpected response type (attempt %d/%d): %d", i+1, maxRetries, msgType)
			if i == maxRetries-1 {
				return fmt.Errorf("unexpected response type after %d attempts: %d", maxRetries, msgType)
			}
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}

		// Parse response
		response := &pb.ShuffleResponse{}
		if err := proto.Unmarshal(responseData, response); err != nil {
			log.Printf("Failed to unmarshal response (attempt %d/%d): %v", i+1, maxRetries, err)
			if i == maxRetries-1 {
				return fmt.Errorf("failed to unmarshal response after %d attempts: %v", maxRetries, err)
			}
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}

		if !response.Success {
			return fmt.Errorf("shuffle failed: %s", response.Error)
		}

		// Success
		return nil
	}
	
	return fmt.Errorf("failed to send shuffle request after %d attempts", maxRetries)
}

// sendMapTaskResponse sends a map task response to the computation manager
func (h *MapReduceHandler) sendMapTaskResponse(response *pb.MapTaskResponse) {
	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}

	// Try to send the response with retries
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		// Connect to computation manager
		conn, err := net.Dial("tcp", h.node.computationAddr)
		if err != nil {
			log.Printf("Failed to connect to computation manager (attempt %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}
		
		// Send response
		err = common.WriteMessage(conn, common.MsgTypeMapTaskComplete, responseData)
		conn.Close() // Close connection after sending
		
		if err != nil {
			log.Printf("Failed to send response (attempt %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}
		
		// Success
		return
	}
	
	log.Printf("Failed to send map task response after %d attempts", maxRetries)
}

// sendReduceTaskResponse sends a reduce task response to the computation manager
func (h *MapReduceHandler) sendReduceTaskResponse(response *pb.ReduceTaskResponse) {
	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}

	// Try to send the response with retries
	maxRetries := 5 // Increased from 3 to 5
	maxBackoff := 10 // Maximum backoff in seconds
	
	for i := 0; i < maxRetries; i++ {
		// Connect to computation manager
		log.Printf("Attempting to connect to computation manager at %s (attempt %d/%d)",
			h.node.computationAddr, i+1, maxRetries)
			
		conn, err := net.Dial("tcp", h.node.computationAddr)
		if err != nil {
			log.Printf("Failed to connect to computation manager (attempt %d/%d): %v", i+1, maxRetries, err)
			
			// Calculate backoff time (with a cap)
			backoff := time.Duration(i+1) * time.Second
			if backoff > time.Duration(maxBackoff) * time.Second {
				backoff = time.Duration(maxBackoff) * time.Second
			}
			
			log.Printf("Retrying in %v seconds", backoff.Seconds())
			time.Sleep(backoff)
			continue
		}
		
		log.Printf("Successfully connected to computation manager, sending response")
		
		// Send response
		err = common.WriteMessage(conn, common.MsgTypeReduceTaskComplete, responseData)
		conn.Close() // Close connection after sending
		
		if err != nil {
			// Check if it's a broken pipe error
			if strings.Contains(err.Error(), "broken pipe") {
				log.Printf("Broken pipe error when sending response (attempt %d/%d): %v", i+1, maxRetries, err)
				log.Printf("This is likely due to the computation manager closing the connection")
				
				// For broken pipe errors, we can consider the message as sent
				// since the computation manager likely already received it
				if i >= 1 {  // If this is at least the second attempt, consider it successful
					log.Printf("Considering response as sent after broken pipe on retry %d", i+1)
					return
				}
			} else {
				log.Printf("Failed to send response (attempt %d/%d): %v", i+1, maxRetries, err)
			}
			
			// Calculate backoff time (with a cap)
			backoff := time.Duration(i+1) * time.Second
			if backoff > time.Duration(maxBackoff) * time.Second {
				backoff = time.Duration(maxBackoff) * time.Second
			}
			
			log.Printf("Retrying in %v seconds", backoff.Seconds())
			time.Sleep(backoff)
			continue
		}
		
		// Success
		log.Printf("Successfully sent reduce task response")
		return
	}
	
	log.Printf("Failed to send reduce task response after %d attempts", maxRetries)
}

// storeOutputFile stores the output file in the DFS
func (h *MapReduceHandler) storeOutputFile(localFile, dfsFile string) error {
	// Read file
	data, err := ioutil.ReadFile(localFile)
	if err != nil {
		log.Printf("ERROR: Failed to read local file %s: %v", localFile, err)
		return fmt.Errorf("failed to read file: %v", err)
	}

	log.Printf("Storing output file %s in DFS as %s (size: %d bytes)", localFile, dfsFile, len(data))
	
	// Check if file exists and has content
	fileInfo, err := os.Stat(localFile)
	if err != nil {
		log.Printf("ERROR: Failed to stat local file %s: %v", localFile, err)
	} else {
		log.Printf("Local file info: Size=%d, ModTime=%s", fileInfo.Size(), fileInfo.ModTime())
	}

	// Connect to controller to get storage nodes
	log.Printf("Connecting to controller at %s", h.node.controllerAddr)
	conn, err := net.Dial("tcp", h.node.controllerAddr)
	if err != nil {
		log.Printf("ERROR: Failed to connect to controller at %s: %v", h.node.controllerAddr, err)
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()
	log.Printf("Successfully connected to controller")

	// Create storage request
	request := &pb.StorageRequest{
		Filename:  dfsFile,
		FileSize:  uint64(len(data)),
		ChunkSize: 64 * 1024 * 1024, // 1MB chunks
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeStorageRequest, requestData); err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		// Check if it's an EOF error
		if strings.Contains(err.Error(), "EOF") {
			log.Printf("EOF error when reading storage response: %v", err)
			log.Printf("This is likely due to the controller closing the connection")
			
			// For EOF errors, we'll try to continue with a default behavior
			log.Printf("Attempting to store file directly on a random node")
			
			// Try to store on a random node (including this one)
			// Use full IP addresses for nodes instead of just port numbers
			nodes := []string{h.node.nodeID, "10.0.2.21:8001", "10.0.2.22:8002", "10.0.2.23:8003"}
			for _, nodeID := range nodes {
				if nodeID == h.node.nodeID {
					continue  // Skip self, we'll try other nodes first
				}
				
				// Use the full node address directly
				nodeAddr := nodeID
				
				log.Printf("Trying to store file on node %s", nodeAddr)
				
				// Connect to storage node
				nodeConn, err := net.Dial("tcp", nodeAddr)
				if err != nil {
					log.Printf("Failed to connect to node %s: %v", nodeAddr, err)
					continue
				}
				
				// Create chunk store request
				chunkRequest := &pb.ChunkStoreRequest{
					Filename:     dfsFile,
					ChunkNumber:  0,
					Data:         data,
				}
				
				// Serialize request
				chunkRequestData, err := proto.Marshal(chunkRequest)
				if err != nil {
					nodeConn.Close()
					log.Printf("Failed to marshal chunk request: %v", err)
					continue
				}
				
				// Send request
				err = common.WriteMessage(nodeConn, common.MsgTypeChunkStore, chunkRequestData)
				if err != nil {
					nodeConn.Close()
					log.Printf("Failed to send chunk request: %v", err)
					continue
				}
				
				// Read response
				chunkMsgType, chunkResponseData, err := common.ReadMessage(nodeConn)
				nodeConn.Close()
				
				if err != nil {
					log.Printf("Failed to read chunk response: %v", err)
					continue
				}
				
				// There's no specific chunk store response type defined, so we'll check for any valid response
				if chunkMsgType < 1 || chunkMsgType > 32 {
					log.Printf("Unexpected chunk response type: %d", chunkMsgType)
					continue
				}
				
				// Parse response
				chunkResponse := &pb.ChunkStoreResponse{}
				if err := proto.Unmarshal(chunkResponseData, chunkResponse); err != nil {
					log.Printf("Failed to unmarshal chunk response: %v", err)
					continue
				}
				
				if !chunkResponse.Success {
					log.Printf("Chunk store failed: %s", chunkResponse.Error)
					continue
				}
				
				log.Printf("Successfully stored file on node %s after EOF error", nodeAddr)
				return nil
			}
			
			return fmt.Errorf("failed to store file after EOF error")
		}
		
		return fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeStorageResponse {
		return fmt.Errorf("unexpected response type: %d", msgType)
	}

	// Parse response
	response := &pb.StorageResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if response.Error != "" {
		return fmt.Errorf("storage request failed: %s", response.Error)
	}

	// If file is small enough to fit in a single chunk, store it directly
	if len(data) <= int(request.ChunkSize) {
		// Get first storage node
		if len(response.ChunkPlacements) == 0 || len(response.ChunkPlacements[0].StorageNodes) == 0 {
			return fmt.Errorf("no storage nodes assigned for file")
		}

		nodeID := response.ChunkPlacements[0].StorageNodes[0]
		
		// If this is the assigned node, store locally
		if nodeID == h.node.nodeID {
			chunkDir := filepath.Join(h.node.dataDir, "chunks")
			if err := os.MkdirAll(chunkDir, 0755); err != nil {
				return fmt.Errorf("failed to create chunks directory: %v", err)
			}

			chunkPath := filepath.Join(chunkDir, fmt.Sprintf("%s.%d", dfsFile, 0))
			if err := ioutil.WriteFile(chunkPath, data, 0644); err != nil {
				return fmt.Errorf("failed to write chunk: %v", err)
			}
			
			log.Printf("Stored chunk 0 of %s locally", dfsFile)
			return nil
		}

		// Otherwise, send to assigned node
		// Get node address - use the full address directly
		nodeAddr := nodeID
		
		// Log the node address we're using
		log.Printf("Using full node address: %s for chunk storage", nodeAddr)
		
		log.Printf("Connecting to storage node at %s for chunk storage", nodeAddr)
		
		// Connect to storage node
		nodeConn, err := net.Dial("tcp", nodeAddr)
		if err != nil {
			log.Printf("ERROR: Failed to connect to storage node at %s: %v", nodeAddr, err)
			return fmt.Errorf("failed to connect to storage node: %v", err)
		}
		defer nodeConn.Close()
		
		log.Printf("Successfully connected to storage node at %s", nodeAddr)

		// Create chunk store request
		chunkRequest := &pb.ChunkStoreRequest{
			Filename:     dfsFile,
			ChunkNumber:  0,
			Data:         data,
			ReplicaNodes: []string{}, // No replicas for now
		}

		// Serialize request
		chunkRequestData, err := proto.Marshal(chunkRequest)
		if err != nil {
			return fmt.Errorf("failed to marshal chunk request: %v", err)
		}

		// Send request
		if err := common.WriteMessage(nodeConn, common.MsgTypeChunkStore, chunkRequestData); err != nil {
			return fmt.Errorf("failed to send chunk request: %v", err)
		}

		// Read response
		chunkMsgType, chunkResponseData, err := common.ReadMessage(nodeConn)
		if err != nil {
			return fmt.Errorf("failed to read chunk response: %v", err)
		}

		if chunkMsgType != common.MsgTypeChunkStore {
			return fmt.Errorf("unexpected chunk response type: %d", chunkMsgType)
		}

		// Parse response
		chunkResponse := &pb.ChunkStoreResponse{}
		if err := proto.Unmarshal(chunkResponseData, chunkResponse); err != nil {
			return fmt.Errorf("failed to unmarshal chunk response: %v", err)
		}

		if !chunkResponse.Success {
			return fmt.Errorf("chunk store failed: %s", chunkResponse.Error)
		}

		log.Printf("Stored chunk 0 of %s on node %s", dfsFile, nodeID)
		return nil
	}

	// For larger files, split into chunks and store each chunk
	// (This implementation is simplified and doesn't handle large files properly)
	return fmt.Errorf("file too large to store: %d bytes", len(data))
}

// verifyExecutable checks if a file is executable
func verifyExecutable(path string) error {
	// Check if file exists
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file: %v", err)
	}
	
	// Check if file is executable (mode & 0111 != 0)
	if info.Mode()&0111 == 0 {
		// Try to make it executable
		if err := os.Chmod(path, 0755); err != nil {
			return fmt.Errorf("failed to make file executable: %v", err)
		}
		
		// Verify again
		info, err = os.Stat(path)
		if err != nil {
			return fmt.Errorf("failed to stat file after chmod: %v", err)
		}
		
		if info.Mode()&0111 == 0 {
			return fmt.Errorf("file is still not executable after chmod")
		}
	}
	
	return nil
}
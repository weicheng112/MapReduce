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

	// Save job binary to temporary file
	binaryPath := filepath.Join(h.tempDir, fmt.Sprintf("job_%s.bin", request.JobId))
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

// HandleReduceTask handles a reduce task request
func (h *MapReduceHandler) HandleReduceTask(data []byte) ([]byte, error) {
	// Parse request
	request := &pb.ReduceTaskRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal reduce task request: %v", err)
	}

	log.Printf("Received reduce task request for job: %s, task: %s, reducer: %d", 
		request.JobId, request.TaskId, request.ReducerNumber)

	// Create active task
	task := &ActiveTask{
		ID:            request.TaskId,
		JobID:         request.JobId,
		Type:          "reduce",
		ReducerNumber: request.ReducerNumber,
		JobBinary:     request.JobBinary,
	}

	// Save job binary to temporary file
	binaryPath := filepath.Join(h.tempDir, fmt.Sprintf("job_%s.bin", request.JobId))
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

	// Create output file
	outputPath := filepath.Join(taskDir, "output.txt")
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

	// Store output file in DFS
	if err := h.storeOutputFile(outputPath, outputFile); err != nil {
		log.Printf("Failed to store output file: %v", err)
		response.Success = false
		response.Error = fmt.Sprintf("Failed to store output file: %v", err)
		h.sendReduceTaskResponse(response)
		return
	}

	// Update response
	response.Success = true
	response.OutputFile = outputFile

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

	// Create command
	cmd := exec.Command(task.BinaryPath, "map", inputFile)
	
	// Create pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	// Start command
	if err := cmd.Start(); err != nil {
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
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("command failed: %v", err)
	}

	return nil
}

// executeReduceFunction executes the reduce function on the shuffle files
func (h *MapReduceHandler) executeReduceFunction(task *ActiveTask, shuffleFiles []string, outputFile string) error {
	// For now, we'll use a simple approach: execute the job binary as a separate process
	// In a more sophisticated implementation, we could use Go plugins

	// Create command
	args := append([]string{"reduce", outputFile}, shuffleFiles...)
	cmd := exec.Command(task.BinaryPath, args...)
	
	// Create pipes for stdout and stderr
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	// Start command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %v", err)
	}

	// Read stderr
	stderrBytes, err := ioutil.ReadAll(stderr)
	if err != nil {
		return fmt.Errorf("failed to read stderr: %v", err)
	}
	if len(stderrBytes) > 0 {
		log.Printf("Reduce task stderr: %s", string(stderrBytes))
	}

	// Wait for command to finish
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("command failed: %v", err)
	}

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
			log.Printf("Failed to read shuffle response (attempt %d/%d): %v", i+1, maxRetries, err)
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
		err = common.WriteMessage(conn, common.MsgTypeReduceTaskComplete, responseData)
		conn.Close() // Close connection after sending
		
		if err != nil {
			log.Printf("Failed to send response (attempt %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
			continue
		}
		
		// Success
		return
	}
	
	log.Printf("Failed to send reduce task response after %d attempts", maxRetries)
}

// storeOutputFile stores the output file in the DFS
func (h *MapReduceHandler) storeOutputFile(localFile, dfsFile string) error {
	// Read file
	data, err := ioutil.ReadFile(localFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	// TODO: Implement storing the file in the DFS
	// For now, just write it to a local file
	outputDir := filepath.Join(h.node.dataDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	outputPath := filepath.Join(outputDir, filepath.Base(dfsFile))
	if err := ioutil.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}
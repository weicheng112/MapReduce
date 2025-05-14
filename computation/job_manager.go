package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// JobManager handles MapReduce job management
type JobManager struct {
	cm        *ComputationManager
	jobsMutex sync.RWMutex
	nodesMutex sync.RWMutex
}

// NewJobManager creates a new job manager
func NewJobManager(cm *ComputationManager) *JobManager {
	return &JobManager{
		cm: cm,
	}
}

// SubmitJob submits a new MapReduce job
func (jm *JobManager) SubmitJob(request *pb.JobSubmitRequest) (*pb.JobSubmitResponse, error) {
	// Generate job ID if not provided
	jobID := request.JobId
	if jobID == "" {
		jobID = jm.cm.generateJobID()
	}

	// Get file information from controller
	fileInfo, err := jm.getFileInfo(request.InputFile)
	if err != nil {
		return &pb.JobSubmitResponse{
			Success: false,
			JobId:   jobID,
			Error:   fmt.Sprintf("Failed to get file info: %v", err),
		}, nil
	}

	// Get chunk locations for data locality
	chunkLocations, err := jm.getChunkLocations(request.InputFile)
	if err != nil {
		log.Printf("Warning: Failed to get chunk locations: %v. Proceeding without data locality optimization.", err)
		// Continue without chunk locations - we'll fall back to load-based scheduling
	}

	// Create a map of chunk number to node IDs
	chunkToNodes := make(map[uint32][]string)
	for _, chunkLoc := range chunkLocations {
		chunkToNodes[chunkLoc.ChunkNumber] = chunkLoc.StorageNodes
		log.Printf("Chunk %d is located on nodes: %v", chunkLoc.ChunkNumber, chunkLoc.StorageNodes)
	}

	// Create job
	job := &Job{
		ID:                  jobID,
		InputFile:           request.InputFile,
		OutputFile:          request.OutputFile,
		JobBinary:           request.JobBinary,
		NumReducers:         request.NumReducers,
		IsTextFile:          request.IsTextFile,
		State:               common.JobStatePending,
		TotalMapTasks:       uint32(fileInfo.NumChunks),
		CompletedMapTasks:   0,
		TotalReduceTasks:    request.NumReducers,
		CompletedReduceTasks: 0,
		MapTasks:            make(map[string]*Task),
		ReduceTasks:         make(map[string]*Task),
		Reducers:            make([]*ReducerInfo, 0),
		ChunkLocations:      chunkToNodes,
		StartTime:           time.Now(),
	}

	// Create map tasks
	for i := uint32(0); i < fileInfo.NumChunks; i++ {
		taskID := jm.cm.generateTaskID()
		job.MapTasks[taskID] = &Task{
			ID:          taskID,
			JobID:       jobID,
			Type:        "map",
			State:       common.TaskStatePending,
			ChunkNumber: i,
		}
	}

	// Select reducer nodes
	reducers, err := jm.selectReducers(int(request.NumReducers))
	if err != nil {
		return &pb.JobSubmitResponse{
			Success: false,
			JobId:   jobID,
			Error:   fmt.Sprintf("Failed to select reducers: %v", err),
		}, nil
	}

	// Create reducer info
	for i, node := range reducers {
		job.Reducers = append(job.Reducers, &ReducerInfo{
			NodeID:        node.ID,
			Address:       node.Address,
			ReducerNumber: uint32(i),
		})

		// Create reduce tasks
		taskID := jm.cm.generateTaskID()
		job.ReduceTasks[taskID] = &Task{
			ID:            taskID,
			JobID:         jobID,
			Type:          "reduce",
			State:         common.TaskStatePending,
			NodeID:        node.ID,
			ReducerNumber: uint32(i),
		}
	}

	// Add job to jobs map
	jm.jobsMutex.Lock()
	jm.cm.jobs[jobID] = job
	jm.jobsMutex.Unlock()

	// Start scheduling map tasks
	go jm.cm.scheduleMapTasks(job)

	// Return response
	return &pb.JobSubmitResponse{
		Success: true,
		JobId:   jobID,
	}, nil
}

// GetJobStatus gets the status of a job
func (jm *JobManager) GetJobStatus(jobID string) (*pb.JobStatusResponse, error) {
	// Get job
	jm.jobsMutex.RLock()
	job, exists := jm.cm.jobs[jobID]
	jm.jobsMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Create response
	response := &pb.JobStatusResponse{
		JobId:               jobID,
		TotalMapTasks:       job.TotalMapTasks,
		CompletedMapTasks:   job.CompletedMapTasks,
		TotalReduceTasks:    job.TotalReduceTasks,
		CompletedReduceTasks: job.CompletedReduceTasks,
		Error:               job.Error,
	}

	// Set state
	switch job.State {
	case common.JobStatePending:
		response.State = pb.JobStatusResponse_PENDING
	case common.JobStateMapping:
		response.State = pb.JobStatusResponse_MAPPING
	case common.JobStateShuffling:
		response.State = pb.JobStatusResponse_SHUFFLING
	case common.JobStateReducing:
		response.State = pb.JobStatusResponse_REDUCING
	case common.JobStateCompleted:
		response.State = pb.JobStatusResponse_COMPLETED
	case common.JobStateFailed:
		response.State = pb.JobStatusResponse_FAILED
	}

	return response, nil
}

// HandleMapTaskComplete handles a map task completion notification
func (jm *JobManager) HandleMapTaskComplete(response *pb.MapTaskResponse) error {
	// Get job
	jm.jobsMutex.Lock()
	defer jm.jobsMutex.Unlock()

	job, exists := jm.cm.jobs[response.JobId]
	if !exists {
		return fmt.Errorf("job not found: %s", response.JobId)
	}

	// Get task
	task, exists := job.MapTasks[response.TaskId]
	if !exists {
		return fmt.Errorf("task not found: %s", response.TaskId)
	}

	// Update task state
	if response.Success {
		task.State = common.TaskStateCompleted
		task.EndTime = time.Now()
		job.CompletedMapTasks++

		// If all map tasks are complete, start reduce phase
		if job.CompletedMapTasks == job.TotalMapTasks {
			job.State = common.JobStateShuffling
			go jm.cm.scheduleReduceTasks(job)
		}
	} else {
		task.State = common.TaskStateFailed
		task.Error = response.Error

		// Reschedule the task
		task.State = common.TaskStatePending
		go jm.cm.scheduleMapTasks(job)
	}

	return nil
}

// HandleReduceTaskComplete handles a reduce task completion notification
func (jm *JobManager) HandleReduceTaskComplete(response *pb.ReduceTaskResponse) error {
	// Get job
	jm.jobsMutex.Lock()
	defer jm.jobsMutex.Unlock()

	job, exists := jm.cm.jobs[response.JobId]
	if !exists {
		return fmt.Errorf("job not found: %s", response.JobId)
	}

	// Get task
	task, exists := job.ReduceTasks[response.TaskId]
	if !exists {
		return fmt.Errorf("task not found: %s", response.TaskId)
	}

	// Update task state
	if response.Success {
		task.State = common.TaskStateCompleted
		task.EndTime = time.Now()
		task.OutputFile = response.OutputFile // Store the output file path
		job.CompletedReduceTasks++

		// If all reduce tasks are complete, combine outputs and mark job as completed
		if job.CompletedReduceTasks == job.TotalReduceTasks {
			log.Printf("All reduce tasks completed for job %s. Combining outputs...", job.ID)
			
			// Combine reducer outputs in a separate goroutine to avoid blocking
			go func(j *Job) {
				if err := jm.combineReducerOutputs(j); err != nil {
					log.Printf("Error combining reducer outputs for job %s: %v", j.ID, err)
					
					// Even if combining fails, the job is still considered complete
					// since all reducers finished their work
					jm.jobsMutex.Lock()
					j.State = common.JobStateCompleted
					j.EndTime = time.Now()
					j.Error = fmt.Sprintf("Job completed but failed to combine outputs: %v", err)
					jm.jobsMutex.Unlock()
				} else {
					log.Printf("Successfully combined reducer outputs for job %s", j.ID)
					
					jm.jobsMutex.Lock()
					j.State = common.JobStateCompleted
					j.EndTime = time.Now()
					jm.jobsMutex.Unlock()
				}
			}(job)
		}
	} else {
		task.State = common.TaskStateFailed
		task.Error = response.Error

		// Reschedule the task
		task.State = common.TaskStatePending
		go jm.cm.scheduleReduceTasks(job)
	}

	return nil
}

// combineReducerOutputs combines the outputs from all reducers into a single output file
func (jm *JobManager) combineReducerOutputs(job *Job) error {
	log.Printf("Combining outputs for job %s", job.ID)
	
	// For word count jobs, we need to combine the counts for the same words
	// Create a map to store the combined counts
	wordCounts := make(map[string]int)
	
	// Retrieve and combine outputs from all reducers
	for i := uint32(0); i < job.TotalReduceTasks; i++ {
		// Get the output file from the reducer
		reducerOutput, err := jm.retrieveReducerOutput(job, i)
		if err != nil {
			return fmt.Errorf("failed to retrieve output from reducer %d: %v", i, err)
		}
		
		// Parse the reducer output and update the word counts
		scanner := bufio.NewScanner(bytes.NewReader(reducerOutput))
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, "\t")
			if len(parts) != 2 {
				log.Printf("Invalid line in reducer %d output: %s", i, line)
				continue
			}
			
			word := parts[0]
			count, err := strconv.Atoi(parts[1])
			if err != nil {
				log.Printf("Invalid count in reducer %d output: %s", i, parts[1])
				continue
			}
			
			// Add the count to the existing count for this word
			wordCounts[word] += count
		}
		
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error scanning reducer %d output: %v", i, err)
		}
	}
	
	// Create a temporary file to store the combined output
	tempFile, err := ioutil.TempFile("", "combined_output_*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()
	
	// Write the combined word counts to the temporary file
	for word, count := range wordCounts {
		if _, err := fmt.Fprintf(tempFile, "%s\t%d\n", word, count); err != nil {
			return fmt.Errorf("failed to write to combined file: %v", err)
		}
	}
	
	// Flush the file to ensure all data is written
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file: %v", err)
	}
	
	// Rewind the file to read from the beginning
	if _, err := tempFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to beginning of temporary file: %v", err)
	}
	
	// Read the combined data
	combinedData, err := ioutil.ReadAll(tempFile)
	if err != nil {
		return fmt.Errorf("failed to read combined data: %v", err)
	}
	
	// Store the combined output in the DFS
	if err := jm.storeOutputFile(job.OutputFile, combinedData); err != nil {
		return fmt.Errorf("failed to store combined output: %v", err)
	}
	
	log.Printf("Successfully combined and stored output for job %s with %d unique words", job.ID, len(wordCounts))
	return nil
}

// retrieveReducerOutput retrieves the output file from a reducer
func (jm *JobManager) retrieveReducerOutput(job *Job, reducerNumber uint32) ([]byte, error) {
	// Find the task for this reducer to get the output file path
	var dfsFilePath string
	for _, task := range job.ReduceTasks {
		if task.ReducerNumber == reducerNumber && task.State == common.TaskStateCompleted {
			dfsFilePath = task.OutputFile
			log.Printf("Found DFS file path for reducer %d: %s", reducerNumber, dfsFilePath)
			break
		}
	}
	
	if dfsFilePath == "" {
		// Fallback to the default path if we couldn't find the task
		dfsFilePath = fmt.Sprintf("__temp_reducer_%s_%d.txt", job.ID, reducerNumber)
		log.Printf("Using default DFS file path for reducer %d: %s", reducerNumber, dfsFilePath)
	}
	
	// Retrieve the file from the DFS
	log.Printf("Retrieving file %s from DFS", dfsFilePath)
	
	// Connect to controller to get chunk locations
	conn, err := net.Dial("tcp", jm.cm.controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()
	
	// Create retrieval request
	request := &pb.RetrievalRequest{
		Filename: dfsFilePath,
	}
	
	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}
	
	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeRetrievalRequest, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	
	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}
	
	if msgType != common.MsgTypeRetrievalResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}
	
	// Parse response
	response := &pb.RetrievalResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}
	
	if response.Error != "" {
		return nil, fmt.Errorf("controller error: %s", response.Error)
	}
	
	// Get chunk locations
	if len(response.Chunks) == 0 {
		return nil, fmt.Errorf("no chunks found for file %s", dfsFilePath)
	}
	
	// For simplicity, we'll assume the file has only one chunk
	chunk := response.Chunks[0]
	if len(chunk.StorageNodes) == 0 {
		return nil, fmt.Errorf("no storage nodes found for chunk %d", chunk.ChunkNumber)
	}
	
	// Try each node until successful
	var lastErr error
	for _, nodeID := range chunk.StorageNodes {
		// Connect to storage node - nodeID should already be a full address (hostname:port)
		nodeAddr := nodeID
		
		log.Printf("Connecting to storage node %s to retrieve chunk %d", nodeAddr, chunk.ChunkNumber)
		
		nodeConn, err := net.Dial("tcp", nodeAddr)
		if err != nil {
			lastErr = fmt.Errorf("failed to connect to storage node: %v", err)
			log.Printf("Failed to connect to storage node %s: %v", nodeAddr, err)
			continue
		}
		
		// Create chunk retrieve request
		chunkRequest := &pb.ChunkRetrieveRequest{
			Filename:    dfsFilePath,
			ChunkNumber: chunk.ChunkNumber,
		}
		
		// Serialize request
		chunkRequestData, err := proto.Marshal(chunkRequest)
		if err != nil {
			nodeConn.Close()
			lastErr = fmt.Errorf("failed to marshal chunk request: %v", err)
			continue
		}
		
		// Send request
		if err := common.WriteMessage(nodeConn, common.MsgTypeChunkRetrieve, chunkRequestData); err != nil {
			nodeConn.Close()
			lastErr = fmt.Errorf("failed to send chunk request: %v", err)
			continue
		}
		
		// Read response
		chunkMsgType, chunkResponseData, err := common.ReadMessage(nodeConn)
		nodeConn.Close()
		
		if err != nil {
			lastErr = fmt.Errorf("failed to read chunk response: %v", err)
			continue
		}
		
		if chunkMsgType != common.MsgTypeChunkRetrieve {
			lastErr = fmt.Errorf("unexpected chunk response type: %d", chunkMsgType)
			continue
		}
		
		// Parse response
		chunkResponse := &pb.ChunkRetrieveResponse{}
		if err := proto.Unmarshal(chunkResponseData, chunkResponse); err != nil {
			lastErr = fmt.Errorf("failed to unmarshal chunk response: %v", err)
			continue
		}
		
		if chunkResponse.Error != "" {
			lastErr = fmt.Errorf("storage node error: %s", chunkResponse.Error)
			continue
		}
		
		// Success!
		log.Printf("Successfully retrieved chunk %d from node %s (size: %d bytes)",
			chunk.ChunkNumber, nodeID, len(chunkResponse.Data))
		return chunkResponse.Data, nil
	}
	
	// If we get here, all nodes failed
	return nil, fmt.Errorf("failed to retrieve chunk from all nodes: %v", lastErr)
}

// storeOutputFile stores the combined output file in the DFS
func (jm *JobManager) storeOutputFile(filename string, data []byte) error {
	// Create a directory for the final output if it doesn't exist
	outputDir := filepath.Join(jm.cm.dataDir, "final_outputs")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}
	
	// Create the output file
	outputPath := filepath.Join(outputDir, filename)
	log.Printf("Storing combined output file at %s (size: %d bytes)", outputPath, len(data))
	
	// Write the data to the file
	if err := ioutil.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write output file: %v", err)
	}
	
	// Now store the file in the DFS using the controller
	// Connect to controller
	conn, err := net.Dial("tcp", jm.cm.controllerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()
	
	// Create storage request
	request := &pb.StorageRequest{
		Filename:  filename,
		FileSize:  uint64(len(data)),
		ChunkSize: 1024 * 1024, // 1MB chunks
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
		
		// Connect to storage node - nodeID should already be a full address (hostname:port)
		nodeAddr := nodeID
		
		log.Printf("Connecting to storage node %s to store output file", nodeAddr)
		nodeConn, err := net.Dial("tcp", nodeAddr)
		if err != nil {
			return fmt.Errorf("failed to connect to storage node: %v", err)
		}
		defer nodeConn.Close()
		
		// Create chunk store request
		chunkRequest := &pb.ChunkStoreRequest{
			Filename:     filename,
			ChunkNumber:  0,
			Data:         data,
			ReplicaNodes: response.ChunkPlacements[0].StorageNodes[1:],
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
		
		log.Printf("Successfully stored combined output file in DFS: %s", filename)
		return nil
	}
	
	// For larger files, split into chunks and store each chunk
	// (This implementation is simplified and doesn't handle large files properly)
	return fmt.Errorf("file too large to store: %d bytes", len(data))
}

// HandleComputeHeartbeat handles a compute node heartbeat
func (jm *JobManager) HandleComputeHeartbeat(heartbeat *pb.ComputeNodeHeartbeat) error {
	// Update or create compute node
	jm.nodesMutex.Lock()
	defer jm.nodesMutex.Unlock()

	// Use full address (hostname:port) as the node ID for consistent matching with controller
	fullNodeID := heartbeat.NodeHostname + ":" + heartbeat.NodeId
	
	// Log the node registration with full ID
	log.Printf("Registering compute node with full ID: %s", fullNodeID)
	
	node, exists := jm.cm.nodes[fullNodeID]
	if !exists {
		node = &ComputeNode{
			ID:      fullNodeID,
			Address: fullNodeID,
		}
		jm.cm.nodes[fullNodeID] = node
	}

	// Update node information
	node.CPUCores = heartbeat.CpuCores
	node.MemoryAvailable = heartbeat.MemoryAvailable
	node.ActiveTasks = heartbeat.ActiveTasks
	node.ActiveJobs = heartbeat.ActiveJobIds
	node.LastHeartbeat = time.Now()

	return nil
}

// getFileInfo gets information about a file from the controller
func (jm *JobManager) getFileInfo(filename string) (*pb.FileInfo, error) {
	// Connect to controller
	conn, err := net.Dial("tcp", jm.cm.controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create list files request
	request := &pb.ListFilesRequest{}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeListRequest, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeListResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	// Parse response
	response := &pb.ListFilesResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	// Find file
	for _, file := range response.Files {
		if file.Filename == filename {
			return file, nil
		}
	}

	return nil, fmt.Errorf("file not found: %s", filename)
}

// getChunkLocations gets the locations of chunks for a file from the controller
func (jm *JobManager) getChunkLocations(filename string) ([]*pb.ChunkLocation, error) {
	// Connect to controller
	conn, err := net.Dial("tcp", jm.cm.controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	// Create retrieval request
	request := &pb.RetrievalRequest{
		Filename: filename,
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeRetrievalRequest, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeRetrievalResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	// Parse response
	response := &pb.RetrievalResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if response.Error != "" {
		return nil, fmt.Errorf("retrieval error: %s", response.Error)
	}

	return response.Chunks, nil
}

// selectReducers selects nodes to run reduce tasks
func (jm *JobManager) selectReducers(numReducers int) ([]*ComputeNode, error) {
	// Get all nodes
	jm.nodesMutex.RLock()
	defer jm.nodesMutex.RUnlock()

	if len(jm.cm.nodes) < numReducers {
		return nil, fmt.Errorf("not enough nodes available: %d < %d", len(jm.cm.nodes), numReducers)
	}

	// Sort nodes by load (active tasks)
	var nodes []*ComputeNode
	for _, node := range jm.cm.nodes {
		nodes = append(nodes, node)
	}

	// Sort nodes by active tasks (ascending)
	// This is a simple implementation; you might want to use a more sophisticated algorithm
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if nodes[i].ActiveTasks > nodes[j].ActiveTasks {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}

	// Select top N nodes
	return nodes[:numReducers], nil
}
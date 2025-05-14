package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// ComputationManager represents the computation manager node
type ComputationManager struct {
	port           string
	dataDir        string
	controllerAddr string
	listener       net.Listener
	jobs           map[string]*Job // Map of job ID to job
	jobManager     *JobManager     // Job manager
	nodes          map[string]*ComputeNode // Map of node ID to compute node
	nodesMutex     sync.RWMutex    // Mutex for nodes map
	jobsMutex      sync.RWMutex    // Mutex for jobs map
}

// ComputeNode represents a node that can perform computation tasks
type ComputeNode struct {
	ID             string
	Address        string
	CPUCores       uint32
	MemoryAvailable uint64
	ActiveTasks    uint32
	ActiveJobs     []string
	LastHeartbeat  time.Time
}

// Job represents a MapReduce job
type Job struct {
	ID             string
	InputFile      string
	OutputFile     string
	JobBinary      []byte
	NumReducers    uint32
	IsTextFile     bool
	State          string
	TotalMapTasks  uint32
	CompletedMapTasks uint32
	TotalReduceTasks uint32
	CompletedReduceTasks uint32
	Error          string
	MapTasks       map[string]*Task // Map of task ID to task
	ReduceTasks    map[string]*Task // Map of task ID to task
	Reducers       []*ReducerInfo   // List of reducers
	ChunkLocations map[uint32][]string // Map of chunk number to list of node IDs that have the chunk
	StartTime      time.Time
	EndTime        time.Time
}

// ReducerInfo represents information about a reducer
type ReducerInfo struct {
	NodeID         string
	Address        string
	ReducerNumber  uint32
}

// Task represents a map or reduce task
type Task struct {
	ID             string
	JobID          string
	Type           string // "map" or "reduce"
	State          string // "pending", "running", "completed", "failed"
	NodeID         string // ID of the node running this task
	StartTime      time.Time
	EndTime        time.Time
	ChunkNumber    uint32 // For map tasks
	ReducerNumber  uint32 // For reduce tasks
	Error          string
	OutputFile     string // Path to the output file (for reduce tasks)
}

// NewComputationManager creates a new computation manager
func NewComputationManager(port, dataDir, controllerAddr string) *ComputationManager {
	return &ComputationManager{
		port:           port,
		dataDir:        dataDir,
		controllerAddr: controllerAddr,
		jobs:           make(map[string]*Job),
		nodes:          make(map[string]*ComputeNode),
	}
}

// Start starts the computation manager
func (cm *ComputationManager) Start() error {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cm.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// Start listening for connections
	listener, err := net.Listen("tcp", ":"+cm.port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", cm.port, err)
	}
	cm.listener = listener

	log.Printf("Computation Manager started on port %s", cm.port)

	// Handle incoming connections
	go cm.handleConnections()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cm.listener.Close()
		os.Exit(0)
	}()

	return nil
}

// handleConnections handles incoming connections
func (cm *ComputationManager) handleConnections() {
	for {
		conn, err := cm.listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go cm.handleConnection(conn)
	}
}

// handleConnection handles a single connection
func (cm *ComputationManager) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read message type
	msgType, data, err := common.ReadMessage(conn)
	if err != nil {
		log.Printf("Error reading message: %v", err)
		return
	}

	// Handle message based on type
	var response []byte
	var responseType byte
	
	switch msgType {
	case common.MsgTypeJobSubmit:
		response, err = cm.handleJobSubmit(data)
		responseType = common.MsgTypeJobSubmitResponse
	case common.MsgTypeJobStatus:
		response, err = cm.handleJobStatus(data)
		responseType = common.MsgTypeJobStatusResponse
	case common.MsgTypeMapTaskComplete:
		response, err = cm.handleMapTaskComplete(data)
		// No response needed
	case common.MsgTypeReduceTaskComplete:
		response, err = cm.handleReduceTaskComplete(data)
		// No response needed
	case common.MsgTypeComputeHeartbeat:
		err = cm.handleComputeHeartbeat(data)
	case common.MsgTypeShuffle:
		response, err = cm.handleShuffle(data)
		responseType = common.MsgTypeShuffleResponse
	default:
		err = fmt.Errorf("unknown message type: %d", msgType)
	}

	if err != nil {
		log.Printf("Error handling message: %v", err)
		return
	}

	// Send response if needed
	if response != nil {
		if err := common.WriteMessage(conn, responseType, response); err != nil {
			log.Printf("Error sending response: %v", err)
		}
	}
}

// handleJobSubmit handles a job submission request
func (cm *ComputationManager) handleJobSubmit(data []byte) ([]byte, error) {
	// Parse request
	request := &pb.JobSubmitRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job submit request: %v", err)
	}

	log.Printf("Received job submission request for input file: %s", request.InputFile)

	// Create job manager if not exists
	if cm.jobManager == nil {
		cm.jobManager = NewJobManager(cm)
	}

	// Submit job
	response, err := cm.jobManager.SubmitJob(request)
	if err != nil {
		return nil, fmt.Errorf("failed to submit job: %v", err)
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	log.Printf("Job submitted successfully: %s", response.JobId)
	return responseData, nil
}

// handleJobStatus handles a job status request
func (cm *ComputationManager) handleJobStatus(data []byte) ([]byte, error) {
	// Parse request
	request := &pb.JobStatusRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job status request: %v", err)
	}

	log.Printf("Received job status request for job: %s", request.JobId)

	// Get job status
	if cm.jobManager == nil {
		return nil, fmt.Errorf("job manager not initialized")
	}

	response, err := cm.jobManager.GetJobStatus(request.JobId)
	if err != nil {
		return nil, fmt.Errorf("failed to get job status: %v", err)
	}

	// Serialize response
	responseData, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %v", err)
	}

	log.Printf("Job status: %s, Map: %d/%d, Reduce: %d/%d",
		response.State.String(),
		response.CompletedMapTasks,
		response.TotalMapTasks,
		response.CompletedReduceTasks,
		response.TotalReduceTasks)
	
	return responseData, nil
}

// handleMapTaskComplete handles a map task completion notification
func (cm *ComputationManager) handleMapTaskComplete(data []byte) ([]byte, error) {
	// Parse request
	response := &pb.MapTaskResponse{}
	if err := proto.Unmarshal(data, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal map task response: %v", err)
	}

	log.Printf("Received map task completion for job: %s, task: %s, success: %v",
		response.JobId, response.TaskId, response.Success)

	// Handle map task completion
	if cm.jobManager == nil {
		return nil, fmt.Errorf("job manager not initialized")
	}

	if err := cm.jobManager.HandleMapTaskComplete(response); err != nil {
		return nil, fmt.Errorf("failed to handle map task completion: %v", err)
	}

	// No response needed
	return nil, nil
}

// handleReduceTaskComplete handles a reduce task completion notification
func (cm *ComputationManager) handleReduceTaskComplete(data []byte) ([]byte, error) {
	// Parse request
	response := &pb.ReduceTaskResponse{}
	if err := proto.Unmarshal(data, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal reduce task response: %v", err)
	}

	log.Printf("Received reduce task completion for job: %s, task: %s, success: %v",
		response.JobId, response.TaskId, response.Success)

	// Handle reduce task completion
	if cm.jobManager == nil {
		return nil, fmt.Errorf("job manager not initialized")
	}

	if err := cm.jobManager.HandleReduceTaskComplete(response); err != nil {
		return nil, fmt.Errorf("failed to handle reduce task completion: %v", err)
	}

	// No response needed
	return nil, nil
}

// handleComputeHeartbeat handles a compute node heartbeat
func (cm *ComputationManager) handleComputeHeartbeat(data []byte) error {
	// Parse request
	heartbeat := &pb.ComputeNodeHeartbeat{}
	if err := proto.Unmarshal(data, heartbeat); err != nil {
		return fmt.Errorf("failed to unmarshal compute heartbeat: %v", err)
	}

	// log.Printf("Received heartbeat from compute node: %s",
	// 	heartbeat.NodeId)

	// Handle compute heartbeat
	if cm.jobManager == nil {
		cm.jobManager = NewJobManager(cm)
	}

	if err := cm.jobManager.HandleComputeHeartbeat(heartbeat); err != nil {
		return fmt.Errorf("failed to handle compute heartbeat: %v", err)
	}

	return nil
}

// handleShuffle handles a shuffle request from a storage node
func (cm *ComputationManager) handleShuffle(data []byte) ([]byte, error) {
	// Parse request
	request := &pb.ShuffleRequest{}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, fmt.Errorf("failed to unmarshal shuffle request: %v", err)
	}

	log.Printf("Received shuffle request for job: %s, mapper: %s, reducer: %d",
		request.JobId, request.MapperId, request.ReducerNumber)

	// Get job
	cm.jobsMutex.RLock()
	job, exists := cm.jobs[request.JobId]
	cm.jobsMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job not found: %s", request.JobId)
	}

	// Find the reducer node for this reducer number
	var reducerAddr string
	for _, reducer := range job.Reducers {
		if reducer.ReducerNumber == request.ReducerNumber {
			reducerAddr = reducer.Address
			break
		}
	}

	if reducerAddr == "" {
		return nil, fmt.Errorf("reducer not found for number: %d", request.ReducerNumber)
	}

	// Forward shuffle data to the reducer
	if err := cm.forwardShuffleData(reducerAddr, request); err != nil {
		return nil, fmt.Errorf("failed to forward shuffle data: %v", err)
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

// forwardShuffleData forwards shuffle data to a reducer node
func (cm *ComputationManager) forwardShuffleData(reducerAddr string, request *pb.ShuffleRequest) error {
	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Try to send the request with retries
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		// Connect to reducer
		conn, err := net.Dial("tcp", reducerAddr)
		if err != nil {
			log.Printf("Failed to connect to reducer (attempt %d/%d): %v", i+1, maxRetries, err)
			if i == maxRetries-1 {
				return fmt.Errorf("failed to connect to reducer after %d attempts: %v", maxRetries, err)
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
	
	return fmt.Errorf("failed to forward shuffle data after %d attempts", maxRetries)
}

// scheduleMapTasks schedules map tasks for a job
func (cm *ComputationManager) scheduleMapTasks(job *Job) error {
	log.Printf("Scheduling map tasks for job: %s", job.ID)

	// Update job state
	job.State = common.JobStateMapping

	// Find pending map tasks
	var pendingTasks []*Task
	for _, task := range job.MapTasks {
		if task.State == common.TaskStatePending {
			pendingTasks = append(pendingTasks, task)
		}
	}

	if len(pendingTasks) == 0 {
		log.Printf("No pending map tasks for job: %s", job.ID)
		return nil
	}

	// For each pending task, find a suitable node and schedule it
	for _, task := range pendingTasks {
		// Find a suitable node
		nodeID, err := cm.findSuitableNode(job.ID, true, task.ChunkNumber)
		if err != nil {
			log.Printf("Failed to find suitable node for map task: %s, error: %v", task.ID, err)
			continue
		}

		// Get node address
		cm.nodesMutex.RLock()
		node, exists := cm.nodes[nodeID]
		cm.nodesMutex.RUnlock()
		if !exists {
			log.Printf("Node not found: %s", nodeID)
			continue
		}

		// Create map task request
		request := &pb.MapTaskRequest{
			JobId:       job.ID,
			TaskId:      task.ID,
			Filename:    job.InputFile,
			ChunkNumber: task.ChunkNumber,
			JobBinary:   job.JobBinary,
			Reducers:    make([]*pb.ReducerInfo, 0),
		}

		// Add reducer info
		for _, reducer := range job.Reducers {
			request.Reducers = append(request.Reducers, &pb.ReducerInfo{
				NodeId:         reducer.NodeID,
				Address:        reducer.Address,
				ReducerNumber:  reducer.ReducerNumber,
			})
		}

		// Send request to node
		if err := cm.sendMapTaskRequest(node.Address, request); err != nil {
			log.Printf("Failed to send map task request to node: %s, error: %v", node.Address, err)
			continue
		}

		// Update task state
		task.State = common.TaskStateRunning
		task.NodeID = nodeID
		task.StartTime = time.Now()

		log.Printf("Scheduled map task: %s on node: %s", task.ID, nodeID)
	}

	return nil
}

// scheduleReduceTasks schedules reduce tasks for a job
func (cm *ComputationManager) scheduleReduceTasks(job *Job) error {
	log.Printf("Scheduling reduce tasks for job: %s", job.ID)

	// Update job state
	job.State = common.JobStateReducing

	// Find pending reduce tasks
	var pendingTasks []*Task
	for _, task := range job.ReduceTasks {
		if task.State == common.TaskStatePending {
			pendingTasks = append(pendingTasks, task)
		}
	}

	if len(pendingTasks) == 0 {
		log.Printf("No pending reduce tasks for job: %s", job.ID)
		return nil
	}

	// For each pending task, schedule it on the assigned reducer node
	for _, task := range pendingTasks {
		// Get reducer info
		var reducer *ReducerInfo
		for _, r := range job.Reducers {
			if r.ReducerNumber == task.ReducerNumber {
				reducer = r
				break
			}
		}

		if reducer == nil {
			log.Printf("Reducer not found for task: %s", task.ID)
			continue
		}

		// Create reduce task request
		request := &pb.ReduceTaskRequest{
			JobId:         job.ID,
			TaskId:        task.ID,
			ReducerNumber: task.ReducerNumber,
			JobBinary:     job.JobBinary,
			OutputFile:    job.OutputFile,
		}

		// Send request to node
		if err := cm.sendReduceTaskRequest(reducer.Address, request); err != nil {
			log.Printf("Failed to send reduce task request to node: %s, error: %v", reducer.Address, err)
			continue
		}

		// Update task state
		task.State = common.TaskStateRunning
		task.NodeID = reducer.NodeID
		task.StartTime = time.Now()

		log.Printf("Scheduled reduce task: %s on node: %s", task.ID, reducer.NodeID)
	}

	return nil
}

// findSuitableNode finds a suitable node to run a task
func (cm *ComputationManager) findSuitableNode(jobID string, isMapTask bool, chunkNumber uint32) (string, error) {
	cm.nodesMutex.RLock()
	defer cm.nodesMutex.RUnlock()

	if len(cm.nodes) == 0 {
		return "", fmt.Errorf("no nodes available")
	}

	// Get job to access chunk locations
	cm.jobsMutex.RLock()
	job, exists := cm.jobs[jobID]
	cm.jobsMutex.RUnlock()

	if !exists {
		return "", fmt.Errorf("job not found: %s", jobID)
	}

	// For map tasks, prefer nodes that have the chunk locally
	if isMapTask {
		// Check if we have chunk location information
		if job.ChunkLocations != nil {
			// Get nodes that have this chunk
			if nodeIDs, ok := job.ChunkLocations[chunkNumber]; ok && len(nodeIDs) > 0 {
				// Find the node with the lowest load among those that have the chunk
				var localNode *ComputeNode
				var minActiveTasks uint32 = ^uint32(0) // Max uint32 value

				// Log all available compute nodes for debugging
				log.Printf("Available compute nodes: %v", func() []string {
					var nodeIDs []string
					for id := range cm.nodes {
						nodeIDs = append(nodeIDs, id)
					}
					return nodeIDs
				}())
				
				for _, nodeID := range nodeIDs {
					// Log the node ID from controller for debugging
					log.Printf("Checking if node ID from controller exists: %s", nodeID)
					
					// Try to find the node by its full ID
					if node, exists := cm.nodes[nodeID]; exists {
						log.Printf("Found node %s in compute nodes with %d active tasks", nodeID, node.ActiveTasks)
						if node.ActiveTasks < minActiveTasks {
							localNode = node
							minActiveTasks = node.ActiveTasks
						}
					} else {
						// Log that the node wasn't found
						log.Printf("Node %s from controller not found in compute nodes", nodeID)
					}
				}

				// If we found a suitable local node, return it
				if localNode != nil {
					log.Printf("Data locality: Assigning map task for chunk %d to node %s that has the data locally",
						chunkNumber, localNode.ID)
					return localNode.ID, nil
				}
			}
		}
		// If we couldn't find a node with local data, fall back to load-based scheduling
		log.Printf("No data locality for chunk %d, falling back to load-based scheduling", chunkNumber)
	}

	// Select node with fewest active tasks
	var selectedNode *ComputeNode
	var minActiveTasks uint32 = ^uint32(0) // Max uint32 value

	for _, node := range cm.nodes {
		if node.ActiveTasks < minActiveTasks {
			selectedNode = node
			minActiveTasks = node.ActiveTasks
		}
	}
	
	if selectedNode == nil {
		return "", fmt.Errorf("no suitable node found")
	}

	return selectedNode.ID, nil
}

// sendMapTaskRequest sends a map task request to a node
func (cm *ComputationManager) sendMapTaskRequest(nodeAddr string, request *pb.MapTaskRequest) error {
	// Connect to node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to node: %v", err)
	}
	defer conn.Close()

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeMapTask, requestData); err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	// No response expected immediately; node will send MapTaskResponse when task completes
	return nil
}

// sendReduceTaskRequest sends a reduce task request to a node
func (cm *ComputationManager) sendReduceTaskRequest(nodeAddr string, request *pb.ReduceTaskRequest) error {
	// Connect to node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to node: %v", err)
	}
	defer conn.Close()

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeReduceTask, requestData); err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	// No response expected immediately; node will send ReduceTaskResponse when task completes
	return nil
}

// generateTaskID generates a unique task ID
func (cm *ComputationManager) generateTaskID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

// generateJobID generates a unique job ID
func (cm *ComputationManager) generateJobID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func main() {
	// Parse command line arguments
	port := flag.String("port", "8080", "Port to listen on")
	dataDir := flag.String("data", "computation_data", "Directory to store data")
	controllerAddr := flag.String("controller", "localhost:8000", "Controller address")
	flag.Parse()

	// Create computation manager
	cm := NewComputationManager(*port, *dataDir, *controllerAddr)

	// Start computation manager
	if err := cm.Start(); err != nil {
		log.Fatalf("Failed to start computation manager: %v", err)
	}

	// Wait forever
	select {}
}
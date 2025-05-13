package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"dfs/common"
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
	// TODO: Implement job submission
	// 1. Parse the JobSubmitRequest
	// 2. Generate a unique job ID if not provided
	// 3. Create a new Job struct
	// 4. Determine the chunks of the input file
	// 5. Create map tasks for each chunk
	// 6. Select reducer nodes
	// 7. Create reduce tasks
	// 8. Save the job to the jobs map
	// 9. Start scheduling map tasks
	// 10. Return a JobSubmitResponse
	
	return nil, fmt.Errorf("job submission not implemented yet")
}

// handleJobStatus handles a job status request
func (cm *ComputationManager) handleJobStatus(data []byte) ([]byte, error) {
	// TODO: Implement job status
	// 1. Parse the JobStatusRequest
	// 2. Look up the job in the jobs map
	// 3. Create a JobStatusResponse with the current job status
	// 4. Return the response
	
	return nil, fmt.Errorf("job status not implemented yet")
}

// handleMapTaskComplete handles a map task completion notification
func (cm *ComputationManager) handleMapTaskComplete(data []byte) ([]byte, error) {
	// TODO: Implement map task completion
	// 1. Parse the MapTaskResponse
	// 2. Update the job and task status
	// 3. If all map tasks are complete, start the reduce phase
	// 4. Return nil (no response needed)
	
	return nil, fmt.Errorf("map task completion not implemented yet")
}

// handleReduceTaskComplete handles a reduce task completion notification
func (cm *ComputationManager) handleReduceTaskComplete(data []byte) ([]byte, error) {
	// TODO: Implement reduce task completion
	// 1. Parse the ReduceTaskResponse
	// 2. Update the job and task status
	// 3. If all reduce tasks are complete, mark the job as completed
	// 4. Return nil (no response needed)
	
	return nil, fmt.Errorf("reduce task completion not implemented yet")
}

// handleComputeHeartbeat handles a compute node heartbeat
func (cm *ComputationManager) handleComputeHeartbeat(data []byte) error {
	// TODO: Implement compute node heartbeat
	// 1. Parse the ComputeNodeHeartbeat
	// 2. Update or create the compute node in the nodes map
	// 3. Update the node's last heartbeat time
	// 4. Check for any pending tasks that can be assigned to this node
	
	return fmt.Errorf("compute node heartbeat not implemented yet")
}

// scheduleMapTasks schedules map tasks for a job
func (cm *ComputationManager) scheduleMapTasks(job *Job) error {
	// TODO: Implement map task scheduling
	// 1. For each map task in the job
	// 2. Find a suitable node to run the task
	// 3. Send a MapTaskRequest to the node
	// 4. Update the task state to "running"
	
	return fmt.Errorf("map task scheduling not implemented yet")
}

// scheduleReduceTasks schedules reduce tasks for a job
func (cm *ComputationManager) scheduleReduceTasks(job *Job) error {
	// TODO: Implement reduce task scheduling
	// 1. For each reduce task in the job
	// 2. Find a suitable node to run the task
	// 3. Send a ReduceTaskRequest to the node
	// 4. Update the task state to "running"
	
	return fmt.Errorf("reduce task scheduling not implemented yet")
}

// findSuitableNode finds a suitable node to run a task
func (cm *ComputationManager) findSuitableNode(jobID string, isMapTask bool, chunkNumber uint32) (string, error) {
	// TODO: Implement node selection
	// 1. If it's a map task, prefer nodes that have the chunk locally
	// 2. Otherwise, select nodes based on load (fewer active tasks)
	// 3. Return the node ID
	
	return "", fmt.Errorf("node selection not implemented yet")
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
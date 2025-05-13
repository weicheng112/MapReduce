package main

import (
	"fmt"
	"net"
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
		job.CompletedReduceTasks++

		// If all reduce tasks are complete, mark job as completed
		if job.CompletedReduceTasks == job.TotalReduceTasks {
			job.State = common.JobStateCompleted
			job.EndTime = time.Now()
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

// HandleComputeHeartbeat handles a compute node heartbeat
func (jm *JobManager) HandleComputeHeartbeat(heartbeat *pb.ComputeNodeHeartbeat) error {
	// Update or create compute node
	jm.nodesMutex.Lock()
	defer jm.nodesMutex.Unlock()

	node, exists := jm.cm.nodes[heartbeat.NodeId]
	if !exists {
		node = &ComputeNode{
			ID:      heartbeat.NodeId,
			Address: heartbeat.NodeHostname + ":" + heartbeat.NodeId,
		}
		jm.cm.nodes[heartbeat.NodeId] = node
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
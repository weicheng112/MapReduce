package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

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

	log.Printf("Received heartbeat from compute node: %s, cores: %d, memory: %d, active tasks: %d", 
		heartbeat.NodeId, heartbeat.CpuCores, heartbeat.MemoryAvailable, heartbeat.ActiveTasks)

	// Handle compute heartbeat
	if cm.jobManager == nil {
		cm.jobManager = NewJobManager(cm)
	}

	if err := cm.jobManager.HandleComputeHeartbeat(heartbeat); err != nil {
		return fmt.Errorf("failed to handle compute heartbeat: %v", err)
	}

	return nil
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

	// For map tasks, prefer nodes that have the chunk locally
	if isMapTask {
		// TODO: Implement data locality by checking which nodes have the chunk
		// For now, just select the node with the fewest active tasks
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
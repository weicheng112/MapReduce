package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// submitJob submits a MapReduce job to the computation manager
func (c *Client) submitJob(jobBinaryPath, inputFile, outputFile string, numReducers uint32, isTextFile bool) error {
	// Read job binary
	jobBinary, err := ioutil.ReadFile(jobBinaryPath)
	if err != nil {
		return fmt.Errorf("failed to read job binary: %v", err)
	}

	// Connect to computation manager
	conn, err := net.Dial("tcp", c.computationAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to computation manager: %v", err)
	}
	defer conn.Close()

	// Create job submit request
	request := &pb.JobSubmitRequest{
		InputFile:   inputFile,
		OutputFile:  outputFile,
		JobBinary:   jobBinary,
		NumReducers: numReducers,
		IsTextFile:  isTextFile,
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeJobSubmit, requestData); err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeJobSubmitResponse {
		return fmt.Errorf("unexpected response type: %d", msgType)
	}

	// Parse response
	response := &pb.JobSubmitResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("job submission failed: %s", response.Error)
	}

	fmt.Printf("Job submitted successfully. Job ID: %s\n", response.JobId)

	// Monitor job progress
	return c.monitorJob(response.JobId)
}

// monitorJob monitors the progress of a MapReduce job
func (c *Client) monitorJob(jobID string) error {
	fmt.Println("Monitoring job progress...")
	fmt.Println("--------------------------------------------------")
	fmt.Printf("%-15s %-15s %-15s %-15s\n", "Job ID", "Status", "Map Progress", "Reduce Progress")
	fmt.Println("--------------------------------------------------")

	for {
		// Get job status
		status, err := c.getJobStatus(jobID)
		if err != nil {
			return fmt.Errorf("failed to get job status: %v", err)
		}

		// Print job status
		mapProgress := fmt.Sprintf("%d/%d", status.CompletedMapTasks, status.TotalMapTasks)
		reduceProgress := fmt.Sprintf("%d/%d", status.CompletedReduceTasks, status.TotalReduceTasks)
		fmt.Printf("%-15s %-15s %-15s %-15s\n", jobID, status.State.String(), mapProgress, reduceProgress)

		// Check if job is completed or failed
		if status.State == pb.JobStatusResponse_COMPLETED {
			fmt.Println("--------------------------------------------------")
			fmt.Println("Job completed successfully!")
			return nil
		} else if status.State == pb.JobStatusResponse_FAILED {
			fmt.Println("--------------------------------------------------")
			fmt.Printf("Job failed: %s\n", status.Error)
			return fmt.Errorf("job failed: %s", status.Error)
		}

		// Wait before checking again
		time.Sleep(2 * time.Second)
	}
}

// getJobStatus gets the status of a MapReduce job
func (c *Client) getJobStatus(jobID string) (*pb.JobStatusResponse, error) {
	// Connect to computation manager
	conn, err := net.Dial("tcp", c.computationAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to computation manager: %v", err)
	}
	defer conn.Close()

	// Create job status request
	request := &pb.JobStatusRequest{
		JobId: jobID,
	}

	// Serialize request
	requestData, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	// Send request
	if err := common.WriteMessage(conn, common.MsgTypeJobStatus, requestData); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Read response
	msgType, responseData, err := common.ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if msgType != common.MsgTypeJobStatusResponse {
		return nil, fmt.Errorf("unexpected response type: %d", msgType)
	}

	// Parse response
	response := &pb.JobStatusResponse{}
	if err := proto.Unmarshal(responseData, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return response, nil
}

// handleMapReduceCommand handles the mapreduce command
func (c *Client) handleMapReduceCommand() {
	if len(os.Args) < 5 {
		fmt.Println("Usage: client mapreduce <job_binary> <input_file> <output_file> [num_reducers] [is_text_file]")
		os.Exit(1)
	}

	jobBinaryPath := os.Args[2]
	inputFile := os.Args[3]
	outputFile := os.Args[4]

	// Default values
	numReducers := uint32(common.DefaultNumReducers)
	isTextFile := true

	// Parse optional arguments
	if len(os.Args) > 5 {
		var err error
		var n int
		_, err = fmt.Sscanf(os.Args[5], "%d", &n)
		if err == nil && n > 0 {
			numReducers = uint32(n)
		}
	}

	if len(os.Args) > 6 {
		isTextFile = os.Args[6] == "true"
	}

	// Submit job
	if err := c.submitJob(jobBinaryPath, inputFile, outputFile, numReducers, isTextFile); err != nil {
		log.Fatalf("Failed to submit job: %v", err)
	}
}
package main

import (
	"log"
	"net"
	"runtime"
	"time"

	"dfs/common"
	pb "dfs/proto"

	"google.golang.org/protobuf/proto"
)

// sendComputeHeartbeats periodically sends heartbeat messages to the computation manager
func (n *StorageNode) sendComputeHeartbeats() {
	ticker := time.NewTicker(time.Duration(common.HeartbeatInterval) * time.Second)
	for range ticker.C {
		if err := n.sendComputeHeartbeat(); err != nil {
			log.Printf("Failed to send compute heartbeat: %v", err)
		}
	}
}

// sendComputeHeartbeat sends a single heartbeat message to the computation manager
func (n *StorageNode) sendComputeHeartbeat() error {
	// Connect to computation manager
	conn, err := net.Dial("tcp", n.computationAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Get system info
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Create heartbeat message
	ipAddress := getOutboundIP()
	heartbeat := &pb.ComputeNodeHeartbeat{
		NodeId:          n.nodeID,
		NodeHostname:    ipAddress, // Just use the IP address without the port
		CpuCores:        uint32(runtime.NumCPU()),
		MemoryAvailable: memStats.Sys - memStats.HeapAlloc,
		ActiveTasks:     0, // We'll update this when we implement task tracking
		ActiveJobIds:    []string{},
	}

	// Serialize message
	data, err := proto.Marshal(heartbeat)
	if err != nil {
		return err
	}

	// Send to computation manager
	if err := common.WriteMessage(conn, common.MsgTypeComputeHeartbeat, data); err != nil {
		return err
	}

	log.Printf("Sent compute heartbeat to %s", n.computationAddr)
	return nil
}
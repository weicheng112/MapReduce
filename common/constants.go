package common

// Message types for protocol
const (
	// DFS protocol messages
	MsgTypeHeartbeat        byte = 1
	MsgTypeStorageRequest   byte = 2
	MsgTypeStorageResponse  byte = 3
	MsgTypeRetrievalRequest byte = 4
	MsgTypeRetrievalResponse byte = 5
	MsgTypeDeleteRequest    byte = 6
	MsgTypeDeleteResponse   byte = 7
	MsgTypeListRequest      byte = 8
	MsgTypeListResponse     byte = 9
	MsgTypeNodeStatusRequest byte = 10
	MsgTypeNodeStatusResponse byte = 11
	MsgTypeChunkStore       byte = 12
	MsgTypeChunkRetrieve    byte = 13
	
	// MapReduce protocol messages
	MsgTypeJobSubmit        byte = 20
	MsgTypeJobSubmitResponse byte = 21
	MsgTypeJobStatus        byte = 22
	MsgTypeJobStatusResponse byte = 23
	MsgTypeMapTask          byte = 24
	MsgTypeMapTaskResponse  byte = 25
	MsgTypeReduceTask       byte = 26
	MsgTypeReduceTaskResponse byte = 27
	MsgTypeShuffle          byte = 28
	MsgTypeShuffleResponse  byte = 29
	MsgTypeMapTaskComplete  byte = 30
	MsgTypeReduceTaskComplete byte = 31
	MsgTypeComputeHeartbeat byte = 32
)

// Default values
const (
	DefaultChunkSize    = 64 * 1024 * 1024 // 64MB
	DefaultReplication  = 3
	HeartbeatInterval   = 5  // seconds
	HeartbeatTimeout    = 15 // seconds
	
	// MapReduce constants
	DefaultNumReducers  = 3
	MaxTasksPerNode     = 2  // Maximum number of tasks a node can run simultaneously
	TaskTimeout         = 60 // seconds
	ShuffleTimeout      = 30 // seconds
	JobTimeout          = 3600 // seconds (1 hour)
)

// Job states
const (
	JobStatePending   = "PENDING"
	JobStateMapping   = "MAPPING"
	JobStateShuffling = "SHUFFLING"
	JobStateReducing  = "REDUCING"
	JobStateCompleted = "COMPLETED"
	JobStateFailed    = "FAILED"
)

// Task states
const (
	TaskStatePending   = "PENDING"
	TaskStateRunning   = "RUNNING"
	TaskStateCompleted = "COMPLETED"
	TaskStateFailed    = "FAILED"
)
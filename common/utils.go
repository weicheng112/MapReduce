package common

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// WriteMessage writes a message to a connection with a header
// The header format is: [Type (1 byte)][Length (4 bytes)]
// This is followed by the actual message data
func WriteMessage(conn net.Conn, msgType byte, data []byte) error {
	// Create header: message type (1 byte) + message length (4 bytes)
	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:], uint32(len(data)))
	
	// Write header
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}
	
	// Write message data
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}
	
	return nil
}

// ReadMessage reads a message from a connection
// It first reads the header to determine the message type and length,
// then reads the actual message data
func ReadMessage(conn net.Conn) (byte, []byte, error) {
	// Read header
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, fmt.Errorf("failed to read header: %v", err)
	}
	
	// Extract message type and length
	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:])
	
	// Read message data
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return 0, nil, fmt.Errorf("failed to read data: %v", err)
	}
	
	return msgType, data, nil
}

// CalculateChecksum computes a SHA-256 hash of the provided data
// This is used to verify data integrity when storing and retrieving chunks
func CalculateChecksum(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

// VerifyChecksum checks if the provided data matches the expected checksum
func VerifyChecksum(data, expectedChecksum []byte) bool {
	actualChecksum := CalculateChecksum(data)
	return string(actualChecksum) == string(expectedChecksum)
}

// GetAvailableDiskSpace returns the available disk space in bytes for a given path
// This is used by storage nodes to report their available capacity
func GetAvailableDiskSpace(path string) (uint64, error) {
	// Check if path exists and is a directory
	stat, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("failed to get path stats: %v", err)
	}
	
	if !stat.IsDir() {
		return 0, fmt.Errorf("path is not a directory")
	}
	
	// Start with a fixed total capacity of 10GB
	totalCapacity := uint64(10 * 1024 * 1024 * 1024) // 10GB in bytes
	
	// Calculate space used by stored chunks
	var usedSpace uint64
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory: %v", err)
	}
	
	for _, entry := range entries {
		// Skip directories and non-chunk files
		if entry.IsDir() || entry.Name() == "metadata.json" || entry.Name() == "reported_files.json" {
			continue
		}
		
		// Get file info to determine size
		fileInfo, err := entry.Info()
		if err != nil {
			continue
		}
		
		usedSpace += uint64(fileInfo.Size())
	}
	
	// Calculate available space
	availableSpace := totalCapacity
	if usedSpace < totalCapacity {
		availableSpace = totalCapacity - usedSpace
	} else {
		availableSpace = 0
	}
	
	return availableSpace, nil
}

// SplitFile divides a file into chunks of the specified size
// Returns a slice of byte slices, each representing a chunk
// For text files, it ensures chunks are split at line boundaries
func SplitFile(file *os.File, chunkSize int64) ([][]byte, error) {
	// Get file info to determine size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}
	
	// Check if the file is a text file based on extension
	isTextFile := IsTextFile(fileInfo.Name())
	
	if isTextFile {
		return SplitTextFile(file, chunkSize)
	} else {
		return SplitBinaryFile(file, chunkSize)
	}
}

// SplitTextFile divides a text file into chunks, ensuring that chunks are split at line boundaries
func SplitTextFile(file *os.File, chunkSize int64) ([][]byte, error) {
	// Seek to the beginning of the file
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning of file: %v", err)
	}
	
	chunks := make([][]byte, 0)
	reader := bufio.NewReader(file)
	
	currentChunk := make([]byte, 0, chunkSize)
	
	// Read the file line by line
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read line: %v", err)
		}
		
		// If adding this line would exceed the chunk size, store the current chunk
		if int64(len(currentChunk)+len(line)) > chunkSize && len(currentChunk) > 0 {
			// Store the current chunk
			chunks = append(chunks, currentChunk)
			
			// Start a new chunk
			currentChunk = make([]byte, 0, chunkSize)
		}
		
		// Add the line to the current chunk
		currentChunk = append(currentChunk, line...)
		
		// If we've reached the end of the file
		if err == io.EOF {
			break
		}
	}
	
	// Store any remaining data in the last chunk
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}
	
	return chunks, nil
}

// SplitBinaryFile divides a binary file into chunks of the specified size
func SplitBinaryFile(file *os.File, chunkSize int64) ([][]byte, error) {
	// Get file info to determine size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}
	
	fileSize := fileInfo.Size()
	numChunks := (fileSize + chunkSize - 1) / chunkSize // Ceiling division
	chunks := make([][]byte, 0, numChunks)
	
	// Seek to the beginning of the file
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning of file: %v", err)
	}
	
	// Read file in chunks
	for i := int64(0); i < numChunks; i++ {
		// Create a buffer for this chunk
		chunk := make([]byte, chunkSize)
		
		// Read chunk data
		n, err := file.ReadAt(chunk, i*chunkSize)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk %d: %v", i, err)
		}
		
		// If we read less than the chunk size (last chunk or small file)
		if n < len(chunk) {
			chunk = chunk[:n]
		}
		
		chunks = append(chunks, chunk)
	}
	
	return chunks, nil
}

// IsTextFile determines if a file is a text file based on its extension
func IsTextFile(filename string) bool {
	// Common text file extensions
	textExtensions := map[string]bool{
		".txt":  true,
		".log":  true,
		".csv":  true,
		".json": true,
		".xml":  true,
		".html": true,
		".md":   true,
		".go":   true,
		".py":   true,
		".js":   true,
		".c":    true,
		".cpp":  true,
		".h":    true,
		".java": true,
	}
	
	// Get the file extension
	ext := ""
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '.' {
			ext = filename[i:]
			break
		}
	}
	
	return textExtensions[ext]
}

// JoinChunks combines multiple chunks into a single file
func JoinChunks(chunks [][]byte, output *os.File) error {
	for i, chunk := range chunks {
		if _, err := output.Write(chunk); err != nil {
			return fmt.Errorf("failed to write chunk %d: %v", i, err)
		}
	}
	return nil
}
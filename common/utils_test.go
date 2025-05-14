package common

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSplitTextFile(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "splitfile_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test case 1: Small text file with lines that fit within chunk size
	testCase1 := filepath.Join(tempDir, "test1.txt")
	content1 := "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n"
	if err := os.WriteFile(testCase1, []byte(content1), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Test case 2: Text file with lines that exceed chunk size
	testCase2 := filepath.Join(tempDir, "test2.txt")
	content2 := "Short line\n" +
		"This is a very long line that should exceed the chunk size when we set it to a small value for testing purposes. It contains many characters.\n" +
		"Another short line\n" +
		"Yet another very long line that should also exceed the chunk size when we set it to a small value for testing. This ensures we test the boundary condition.\n" +
		"Final short line\n"
	if err := os.WriteFile(testCase2, []byte(content2), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	
	// Test case 3: Text file with a single very long line
	testCase3 := filepath.Join(tempDir, "test3.txt")
	content3 := "This is an extremely long line that is much longer than the chunk size. " +
		"It should be treated as a single chunk even though it exceeds the chunk size. " +
		"This tests that our implementation correctly handles very long lines without breaking them up. " +
		"The line should be preserved as a single unit regardless of its length."
	if err := os.WriteFile(testCase3, []byte(content3), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Run tests
	tests := []struct {
		name      string
		filePath  string
		chunkSize int64
		expected  int // Expected number of chunks
	}{
		{
			name:      "Small file single chunk",
			filePath:  testCase1,
			chunkSize: 100, // Larger than the file size
			expected:  1,    // Should be a single chunk
		},
		{
			name:      "Small file multiple chunks",
			filePath:  testCase1,
			chunkSize: 10, // Small enough to create multiple chunks
			expected:  5,  // Each line becomes its own chunk due to small chunk size
		},
		{
			name:      "File with long lines",
			filePath:  testCase2,
			chunkSize: 50, // Small enough that long lines exceed it
			expected:  5,  // Each line becomes its own chunk due to long lines
		},
		{
			name:      "Single very long line",
			filePath:  testCase3,
			chunkSize: 50, // Much smaller than the line length
			expected:  1,   // Should be a single chunk since we don't break lines
		},
		{
			name:      "Line boundary at chunk boundary",
			filePath:  testCase1,
			chunkSize: 7, // "Line 1\n" is exactly 7 bytes
			expected:  5,  // Each line should be in its own chunk
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Open the test file
			file, err := os.Open(tt.filePath)
			if err != nil {
				t.Fatalf("Failed to open test file: %v", err)
			}
			defer file.Close()

			// Call SplitTextFile
			chunks, err := SplitTextFile(file, tt.chunkSize)
			if err != nil {
				t.Fatalf("SplitTextFile failed: %v", err)
			}

			// Check number of chunks
			if len(chunks) != tt.expected {
				t.Errorf("Expected %d chunks, got %d", tt.expected, len(chunks))
			}

			// Verify that all chunks combined equal the original content
			var combined bytes.Buffer
			for _, chunk := range chunks {
				combined.Write(chunk)
			}

			// Read the original file content
			originalContent, err := os.ReadFile(tt.filePath)
			if err != nil {
				t.Fatalf("Failed to read original file: %v", err)
			}

			// Compare combined chunks with original content
			if !bytes.Equal(combined.Bytes(), originalContent) {
				t.Errorf("Combined chunks do not match original content")
			}

			// Verify that each chunk (except possibly the last one) ends with a newline
			for i, chunk := range chunks {
				if i < len(chunks)-1 && len(chunk) > 0 && chunk[len(chunk)-1] != '\n' {
					t.Errorf("Chunk %d does not end with a newline", i)
				}
			}
		})
	}
}

func TestIsTextFile(t *testing.T) {
	tests := []struct {
		filename string
		expected bool
	}{
		{"file.txt", true},
		{"file.log", true},
		{"file.csv", true},
		{"file.json", true},
		{"file.xml", true},
		{"file.html", true},
		{"file.md", true},
		{"file.go", true},
		{"file.py", true},
		{"file.js", true},
		{"file.c", true},
		{"file.cpp", true},
		{"file.h", true},
		{"file.java", true},
		{"file.bin", false},
		{"file.exe", false},
		{"file.jpg", false},
		{"file.png", false},
		{"file.pdf", false},
		{"file", false},
		{"file.", false},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			result := IsTextFile(tt.filename)
			if result != tt.expected {
				t.Errorf("IsTextFile(%s) = %v, expected %v", tt.filename, result, tt.expected)
			}
		})
	}
}

func TestSplitFile(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "splitfile_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a text file
	textFile := filepath.Join(tempDir, "test.txt")
	textContent := "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n"
	if err := os.WriteFile(textFile, []byte(textContent), 0644); err != nil {
		t.Fatalf("Failed to write text file: %v", err)
	}

	// Create a binary file
	binaryFile := filepath.Join(tempDir, "test.bin")
	binaryContent := make([]byte, 100)
	for i := range binaryContent {
		binaryContent[i] = byte(i % 256)
	}
	if err := os.WriteFile(binaryFile, binaryContent, 0644); err != nil {
		t.Fatalf("Failed to write binary file: %v", err)
	}

	// Test SplitFile with text file
	t.Run("SplitFile with text file", func(t *testing.T) {
		file, err := os.Open(textFile)
		if err != nil {
			t.Fatalf("Failed to open text file: %v", err)
		}
		defer file.Close()

		chunks, err := SplitFile(file, 10)
		if err != nil {
			t.Fatalf("SplitFile failed: %v", err)
		}

		// Verify that all chunks combined equal the original content
		var combined bytes.Buffer
		for _, chunk := range chunks {
			combined.Write(chunk)
		}

		originalContent, err := os.ReadFile(textFile)
		if err != nil {
			t.Fatalf("Failed to read original file: %v", err)
		}

		if !bytes.Equal(combined.Bytes(), originalContent) {
			t.Errorf("Combined chunks do not match original content")
		}

		// Verify that each chunk (except possibly the last one) ends with a newline
		for i, chunk := range chunks {
			if i < len(chunks)-1 && len(chunk) > 0 && chunk[len(chunk)-1] != '\n' {
				t.Errorf("Chunk %d does not end with a newline", i)
			}
		}
	})

	// Test SplitFile with binary file
	t.Run("SplitFile with binary file", func(t *testing.T) {
		file, err := os.Open(binaryFile)
		if err != nil {
			t.Fatalf("Failed to open binary file: %v", err)
		}
		defer file.Close()

		chunks, err := SplitFile(file, 30)
		if err != nil {
			t.Fatalf("SplitFile failed: %v", err)
		}

		// Verify that all chunks combined equal the original content
		var combined bytes.Buffer
		for _, chunk := range chunks {
			combined.Write(chunk)
		}

		originalContent, err := os.ReadFile(binaryFile)
		if err != nil {
			t.Fatalf("Failed to read original file: %v", err)
		}

		if !bytes.Equal(combined.Bytes(), originalContent) {
			t.Errorf("Combined chunks do not match original content")
		}

		// Verify chunk sizes
		for i, chunk := range chunks {
			if i < len(chunks)-1 && len(chunk) != 30 {
				t.Errorf("Expected chunk %d to be 30 bytes, got %d bytes", i, len(chunk))
			}
		}
	})
}
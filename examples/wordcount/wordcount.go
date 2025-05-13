package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: wordcount [map|reduce] [input_file] [output_files...]")
		os.Exit(1)
	}

	command := os.Args[1]
	inputFile := os.Args[2]

	switch command {
	case "map":
		runMap(inputFile)
	case "reduce":
		outputFile := os.Args[2]
		shuffleFiles := os.Args[3:]
		runReduce(outputFile, shuffleFiles)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}

// runMap reads the input file and emits (word, 1) pairs
func runMap(inputFile string) {
	// Open input file
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Read file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		
		// Split line into words
		words := strings.Fields(line)
		
		// Emit (word, 1) pairs
		for _, word := range words {
			// Clean word (remove punctuation, convert to lowercase)
			word = strings.ToLower(strings.Trim(word, ".,!?\"':;()[]{}"))
			if word == "" {
				continue
			}
			
			// Determine reducer using simple hash
			reducer := hash(word) % 3 // Assuming 3 reducers
			
			// Emit to stdout in format: reducer_num\tkey\tvalue
			fmt.Printf("%d\t%s\t1\n", reducer, word)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}
}

// runReduce reads the shuffle files and counts words
func runReduce(outputFile string, shuffleFiles []string) {
	// Map to store word counts
	wordCounts := make(map[string]int)

	// Process each shuffle file
	for _, shuffleFile := range shuffleFiles {
		file, err := os.Open(shuffleFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening shuffle file: %v\n", err)
			continue
		}

		// Read file line by line
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			
			// Parse line (key\tvalue)
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) != 2 {
				fmt.Fprintf(os.Stderr, "Invalid line format: %s\n", line)
				continue
			}
			
			word := parts[0]
			count, err := parseInt(parts[1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid count: %s\n", parts[1])
				continue
			}
			
			// Update word count
			wordCounts[word] += count
		}

		file.Close()
	}

	// Create output file
	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	// Write word counts to output file
	for word, count := range wordCounts {
		fmt.Fprintf(out, "%s\t%d\n", word, count)
	}
}

// hash computes a simple hash of a string
func hash(s string) uint32 {
	h := uint32(0)
	for i := 0; i < len(s); i++ {
		h = h*31 + uint32(s[i])
	}
	return h
}

// parseInt parses a string to an integer
func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
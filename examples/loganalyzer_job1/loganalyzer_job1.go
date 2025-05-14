package main

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: loganalyzer_job1 [map|reduce] [input_file] [output_files...]")
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

// extractDomain extracts the domain from a URL
func extractDomain(urlStr string) string {
	// Handle URLs without scheme
	if !strings.Contains(urlStr, "://") {
		urlStr = "http://" + urlStr
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return ""
	}

	// Extract domain (host)
	domain := parsedURL.Hostname()
	if domain == "" {
		return ""
	}

	return domain
}

// parseLogEntry parses a log entry line and extracts the URL
func parseLogEntry(line string) (string, error) {
	// Example log format:
	// 192.168.1.1 - - [21/Apr/2023:10:32:15 +0000] "GET /index.html HTTP/1.1" 200 2326 "Mozilla/5.0" 0.023
	
	// Extract request details
	requestRegex := regexp.MustCompile(`"(\S+) (\S+) (\S+)"`)
	requestMatch := requestRegex.FindStringSubmatch(line)
	if len(requestMatch) > 3 {
		urlStr := requestMatch[2]
		return urlStr, nil
	}
	
	return "", fmt.Errorf("could not parse URL from log entry")
}

// runMap reads the input file and emits (domain, 1) pairs
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
		
		// Parse log entry to extract URL
		urlStr, err := parseLogEntry(line)
		if err != nil {
			continue
		}

		// Extract domain
		domain := extractDomain(urlStr)
		if domain == "" {
			continue
		}

		// Determine reducer using simple hash
		reducer := hash(domain) % 3 // Assuming 3 reducers
		
		// Emit to stdout in format: reducer_num\tkey\tvalue
		fmt.Printf("%d\t%s\t1\n", reducer, domain)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}
}

// runReduce reads the shuffle files and counts domains
func runReduce(outputFile string, shuffleFiles []string) {
	// Map to store domain counts
	domainCounts := make(map[string]int)

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
			
			domain := parts[0]
			count, err := parseInt(parts[1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid count: %s\n", parts[1])
				continue
			}
			
			// Update domain count
			domainCounts[domain] += count
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

	// Write domain counts to output file in simple key-value format
	for domain, count := range domainCounts {
		fmt.Fprintf(out, "%s\t%d\n", domain, count)
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
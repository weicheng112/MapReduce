package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: loganalyzer_job2 [map|reduce] [input_file] [output_files...]")
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

// runMap reads the input file (domain counts from job1) and emits all to a single reducer
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
		
		// All lines go to the same reducer
		reducer := 0
		
		// Just pass through the line as-is
		fmt.Printf("%d\t%s\n", reducer, line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}
}

// runReduce reads the shuffle files and produces sorted domain counts
func runReduce(outputFile string, shuffleFiles []string) {
	// Slice to store domain counts
	type DomainCount struct {
		Domain string
		Count  int
	}
	var domainCounts []DomainCount
	
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
			
			// Parse line (domain\tcount)
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) != 2 {
				fmt.Fprintf(os.Stderr, "Invalid line format: %s\n", line)
				continue
			}
			
			domain := parts[0]
			countStr := parts[1]
			
			count, err := strconv.Atoi(countStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid count: %s\n", countStr)
				continue
			}
			
			// Add to domain counts
			domainCounts = append(domainCounts, DomainCount{domain, count})
		}

		file.Close()
	}

	// Sort domain counts by count (descending)
	for i := 0; i < len(domainCounts); i++ {
		for j := i + 1; j < len(domainCounts); j++ {
			if domainCounts[i].Count < domainCounts[j].Count {
				domainCounts[i], domainCounts[j] = domainCounts[j], domainCounts[i]
			}
		}
	}

	// Create output file
	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()
	
	// Write domains sorted by count (descending)
	uniqueDomains := make(map[string]bool)
	for _, dc := range domainCounts {
		uniqueDomains[dc.Domain] = true
	}
	
	// Write top 10 domains
	count := 0
	for _, dc := range domainCounts {
		fmt.Fprintf(out, "%s\t%d\n", dc.Domain, dc.Count)
		count++
		if count >= 10 {
			break
		}
	}
}
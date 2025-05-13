package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LogEntry represents a parsed log entry
type LogEntry struct {
	Timestamp   time.Time
	IP          string
	Method      string
	URL         string
	Status      int
	BytesSent   int
	UserAgent   string
	ProcessTime float64
}

// LogAnalyzer is a MapReduce job that analyzes log files
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: loganalyzer [map|reduce] [input_file] [output_files...]")
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

// parseLogEntry parses a log entry line
func parseLogEntry(line string) (*LogEntry, error) {
	// Example log format:
	// 192.168.1.1 - - [21/Apr/2023:10:32:15 +0000] "GET /index.html HTTP/1.1" 200 2326 "Mozilla/5.0" 0.023
	
	// This is a simplified parser - a real one would be more robust
	ipRegex := regexp.MustCompile(`^(\S+)`)
	timeRegex := regexp.MustCompile(`\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\]`)
	requestRegex := regexp.MustCompile(`"(\S+) (\S+) (\S+)"`)
	statusRegex := regexp.MustCompile(`" (\d{3}) `)
	bytesRegex := regexp.MustCompile(`" \d{3} (\d+)`)
	userAgentRegex := regexp.MustCompile(`"([^"]+)"$`)
	processTimeRegex := regexp.MustCompile(`(\d+\.\d+)$`)

	entry := &LogEntry{}

	// Extract IP
	ipMatch := ipRegex.FindStringSubmatch(line)
	if len(ipMatch) > 1 {
		entry.IP = ipMatch[1]
	}

	// Extract timestamp
	timeMatch := timeRegex.FindStringSubmatch(line)
	if len(timeMatch) > 1 {
		timestamp, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeMatch[1])
		if err == nil {
			entry.Timestamp = timestamp
		}
	}

	// Extract request details
	requestMatch := requestRegex.FindStringSubmatch(line)
	if len(requestMatch) > 3 {
		entry.Method = requestMatch[1]
		entry.URL = requestMatch[2]
	}

	// Extract status code
	statusMatch := statusRegex.FindStringSubmatch(line)
	if len(statusMatch) > 1 {
		status, err := strconv.Atoi(statusMatch[1])
		if err == nil {
			entry.Status = status
		}
	}

	// Extract bytes sent
	bytesMatch := bytesRegex.FindStringSubmatch(line)
	if len(bytesMatch) > 1 {
		bytes, err := strconv.Atoi(bytesMatch[1])
		if err == nil {
			entry.BytesSent = bytes
		}
	}

	// Extract user agent
	userAgentMatch := userAgentRegex.FindStringSubmatch(line)
	if len(userAgentMatch) > 1 {
		entry.UserAgent = userAgentMatch[1]
	}

	// Extract process time
	processTimeMatch := processTimeRegex.FindStringSubmatch(line)
	if len(processTimeMatch) > 1 {
		processTime, err := strconv.ParseFloat(processTimeMatch[1], 64)
		if err == nil {
			entry.ProcessTime = processTime
		}
	}

	return entry, nil
}

// runMap reads the input file and emits various analytics
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
	lineNum := 0
	for scanner.Scan() {
		line := scanner.Text()
		lineNum++

		// Parse log entry
		entry, err := parseLogEntry(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing line %d: %v\n", lineNum, err)
			continue
		}

		// Skip incomplete entries
		if entry.IP == "" || entry.URL == "" {
			continue
		}

		// Emit for URL hit count analysis (reducer 0)
		// Format: reducer_num\tanalysis_type:key\tvalue
		fmt.Printf("0\turl_hits:%s\t1\n", entry.URL)

		// Emit for status code analysis (reducer 1)
		fmt.Printf("1\tstatus_code:%d\t1\n", entry.Status)

		// Emit for IP address analysis (reducer 2)
		fmt.Printf("2\tip_address:%s\t1\n", entry.IP)

		// Emit for hourly traffic analysis (reducer 3)
		hour := entry.Timestamp.Format("2006-01-02:15")
		fmt.Printf("3\thourly_traffic:%s\t1\n", hour)

		// Emit for user agent analysis (reducer 4)
		if entry.UserAgent != "" {
			// Simplify user agent to browser family
			browserFamily := getBrowserFamily(entry.UserAgent)
			fmt.Printf("4\tuser_agent:%s\t1\n", browserFamily)
		}

		// Emit for response size analysis (reducer 5)
		if entry.BytesSent > 0 {
			// Categorize response size
			sizeCategory := getResponseSizeCategory(entry.BytesSent)
			fmt.Printf("5\tresponse_size:%s\t%d\n", sizeCategory, entry.BytesSent)
		}

		// Emit for response time analysis (reducer 6)
		if entry.ProcessTime > 0 {
			fmt.Printf("6\tresponse_time:%s\t%.3f\n", entry.URL, entry.ProcessTime)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}
}

// getBrowserFamily extracts the browser family from a user agent string
func getBrowserFamily(userAgent string) string {
	userAgent = strings.ToLower(userAgent)
	
	if strings.Contains(userAgent, "firefox") {
		return "Firefox"
	} else if strings.Contains(userAgent, "chrome") {
		return "Chrome"
	} else if strings.Contains(userAgent, "safari") {
		return "Safari"
	} else if strings.Contains(userAgent, "edge") {
		return "Edge"
	} else if strings.Contains(userAgent, "msie") || strings.Contains(userAgent, "trident") {
		return "Internet Explorer"
	} else if strings.Contains(userAgent, "bot") || strings.Contains(userAgent, "crawler") || strings.Contains(userAgent, "spider") {
		return "Bot"
	} else {
		return "Other"
	}
}

// getResponseSizeCategory categorizes response size
func getResponseSizeCategory(bytes int) string {
	if bytes < 1024 {
		return "< 1KB"
	} else if bytes < 10*1024 {
		return "1KB-10KB"
	} else if bytes < 100*1024 {
		return "10KB-100KB"
	} else if bytes < 1024*1024 {
		return "100KB-1MB"
	} else {
		return "> 1MB"
	}
}

// runReduce processes the shuffle files and generates analytics
func runReduce(outputFile string, shuffleFiles []string) {
	// Open output file
	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	// Maps for different analytics
	urlHits := make(map[string]int)
	statusCodes := make(map[int]int)
	ipAddresses := make(map[string]int)
	hourlyTraffic := make(map[string]int)
	userAgents := make(map[string]int)
	responseSizes := make(map[string][]int)
	responseTimes := make(map[string][]float64)

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
			
			// Parse line (analysis_type:key\tvalue)
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) != 2 {
				fmt.Fprintf(os.Stderr, "Invalid line format: %s\n", line)
				continue
			}
			
			keyParts := strings.SplitN(parts[0], ":", 2)
			if len(keyParts) != 2 {
				fmt.Fprintf(os.Stderr, "Invalid key format: %s\n", parts[0])
				continue
			}
			
			analysisType := keyParts[0]
			key := keyParts[1]
			value := parts[1]
			
			// Process based on analysis type
			switch analysisType {
			case "url_hits":
				count, _ := strconv.Atoi(value)
				urlHits[key] += count
				
			case "status_code":
				statusCode, _ := strconv.Atoi(key)
				count, _ := strconv.Atoi(value)
				statusCodes[statusCode] += count
				
			case "ip_address":
				count, _ := strconv.Atoi(value)
				ipAddresses[key] += count
				
			case "hourly_traffic":
				count, _ := strconv.Atoi(value)
				hourlyTraffic[key] += count
				
			case "user_agent":
				count, _ := strconv.Atoi(value)
				userAgents[key] += count
				
			case "response_size":
				bytes, _ := strconv.Atoi(value)
				responseSizes[key] = append(responseSizes[key], bytes)
				
			case "response_time":
				seconds, _ := strconv.ParseFloat(value, 64)
				responseTimes[key] = append(responseTimes[key], seconds)
			}
		}
		
		file.Close()
	}

	// Generate report
	fmt.Fprintf(out, "# Log Analysis Report\n\n")
	
	// Top URLs by hits
	fmt.Fprintf(out, "## Top 10 URLs by Hits\n\n")
	fmt.Fprintf(out, "| URL | Hits |\n")
	fmt.Fprintf(out, "|-----|------|\n")
	
	// Sort URLs by hits
	type urlHit struct {
		URL  string
		Hits int
	}
	
	urlHitsList := make([]urlHit, 0, len(urlHits))
	for url, hits := range urlHits {
		urlHitsList = append(urlHitsList, urlHit{url, hits})
	}
	
	sort.Slice(urlHitsList, func(i, j int) bool {
		return urlHitsList[i].Hits > urlHitsList[j].Hits
	})
	
	// Print top 10 URLs
	count := 0
	for _, item := range urlHitsList {
		fmt.Fprintf(out, "| %s | %d |\n", item.URL, item.Hits)
		count++
		if count >= 10 {
			break
		}
	}
	
	// Status code distribution
	fmt.Fprintf(out, "\n## Status Code Distribution\n\n")
	fmt.Fprintf(out, "| Status Code | Count | Description |\n")
	fmt.Fprintf(out, "|-------------|-------|-------------|\n")
	
	statusList := make([]int, 0, len(statusCodes))
	for status := range statusCodes {
		statusList = append(statusList, status)
	}
	
	sort.Ints(statusList)
	
	for _, status := range statusList {
		description := getStatusCodeDescription(status)
		fmt.Fprintf(out, "| %d | %d | %s |\n", status, statusCodes[status], description)
	}
	
	// Top IP addresses
	fmt.Fprintf(out, "\n## Top 10 IP Addresses\n\n")
	fmt.Fprintf(out, "| IP Address | Requests |\n")
	fmt.Fprintf(out, "|------------|----------|\n")
	
	type ipCount struct {
		IP    string
		Count int
	}
	
	ipList := make([]ipCount, 0, len(ipAddresses))
	for ip, count := range ipAddresses {
		ipList = append(ipList, ipCount{ip, count})
	}
	
	sort.Slice(ipList, func(i, j int) bool {
		return ipList[i].Count > ipList[j].Count
	})
	
	count = 0
	for _, item := range ipList {
		fmt.Fprintf(out, "| %s | %d |\n", item.IP, item.Count)
		count++
		if count >= 10 {
			break
		}
	}
	
	// Hourly traffic
	fmt.Fprintf(out, "\n## Hourly Traffic\n\n")
	fmt.Fprintf(out, "| Hour | Requests |\n")
	fmt.Fprintf(out, "|------|----------|\n")
	
	hourList := make([]string, 0, len(hourlyTraffic))
	for hour := range hourlyTraffic {
		hourList = append(hourList, hour)
	}
	
	sort.Strings(hourList)
	
	for _, hour := range hourList {
		fmt.Fprintf(out, "| %s | %d |\n", hour, hourlyTraffic[hour])
	}
	
	// User agent distribution
	fmt.Fprintf(out, "\n## Browser Distribution\n\n")
	fmt.Fprintf(out, "| Browser | Requests | Percentage |\n")
	fmt.Fprintf(out, "|---------|----------|------------|\n")
	
	totalRequests := 0
	for _, count := range userAgents {
		totalRequests += count
	}
	
	type browserCount struct {
		Browser string
		Count   int
	}
	
	browserList := make([]browserCount, 0, len(userAgents))
	for browser, count := range userAgents {
		browserList = append(browserList, browserCount{browser, count})
	}
	
	sort.Slice(browserList, func(i, j int) bool {
		return browserList[i].Count > browserList[j].Count
	})
	
	for _, item := range browserList {
		percentage := float64(item.Count) / float64(totalRequests) * 100
		fmt.Fprintf(out, "| %s | %d | %.2f%% |\n", item.Browser, item.Count, percentage)
	}
	
	// Response size statistics
	fmt.Fprintf(out, "\n## Response Size Statistics\n\n")
	fmt.Fprintf(out, "| Size Category | Count | Average Size (bytes) |\n")
	fmt.Fprintf(out, "|---------------|-------|----------------------|\n")
	
	sizeCategoryList := []string{"< 1KB", "1KB-10KB", "10KB-100KB", "100KB-1MB", "> 1MB"}
	
	for _, category := range sizeCategoryList {
		sizes, exists := responseSizes[category]
		if !exists {
			continue
		}
		
		total := 0
		for _, size := range sizes {
			total += size
		}
		
		average := float64(total) / float64(len(sizes))
		fmt.Fprintf(out, "| %s | %d | %.2f |\n", category, len(sizes), average)
	}
	
	// Response time statistics
	fmt.Fprintf(out, "\n## Top 10 Slowest URLs\n\n")
	fmt.Fprintf(out, "| URL | Average Response Time (seconds) | Max Response Time (seconds) |\n")
	fmt.Fprintf(out, "|-----|----------------------------------|----------------------------|\n")
	
	type urlResponseTime struct {
		URL         string
		AverageTime float64
		MaxTime     float64
	}
	
	urlTimeList := make([]urlResponseTime, 0, len(responseTimes))
	for url, times := range responseTimes {
		if len(times) == 0 {
			continue
		}
		
		total := 0.0
		max := 0.0
		for _, t := range times {
			total += t
			if t > max {
				max = t
			}
		}
		
		average := total / float64(len(times))
		urlTimeList = append(urlTimeList, urlResponseTime{url, average, max})
	}
	
	sort.Slice(urlTimeList, func(i, j int) bool {
		return urlTimeList[i].AverageTime > urlTimeList[j].AverageTime
	})
	
	count = 0
	for _, item := range urlTimeList {
		fmt.Fprintf(out, "| %s | %.3f | %.3f |\n", item.URL, item.AverageTime, item.MaxTime)
		count++
		if count >= 10 {
			break
		}
	}
}

// getStatusCodeDescription returns a description for an HTTP status code
func getStatusCodeDescription(code int) string {
	switch {
	case code >= 100 && code < 200:
		return "Informational"
	case code >= 200 && code < 300:
		return "Success"
	case code >= 300 && code < 400:
		return "Redirection"
	case code >= 400 && code < 500:
		return "Client Error"
	case code >= 500 && code < 600:
		return "Server Error"
	default:
		return "Unknown"
	}
}
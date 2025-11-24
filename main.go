package main

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// PageFetchResult stores the result for each URL
type PageFetchResult struct {
	URL        string
	StatusCode int
	Size       int64
	Error      string
}

// worker pulls URLs from the jobs channel, fetches them, and
// sends PageFetchResult values into the results channel.
func worker(id int, jobs <-chan string, results chan<- PageFetchResult) {
	for url := range jobs {
		fmt.Printf("Worker %d: fetching %s\n", id, url)
		results <- fetchURL(url)
	}
}

// fetchURL actually does the HTTP GET and records status, size, and error.
func fetchURL(url string) PageFetchResult {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return PageFetchResult{
			URL:   url,
			Error: err.Error(),
		}
	}
	defer resp.Body.Close()

	// Read the whole body just to count bytes, discard actual content.
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return PageFetchResult{
			URL:        url,
			StatusCode: resp.StatusCode,
			Size:       n,
			Error:      err.Error(),
		}
	}

	return PageFetchResult{
		URL:        url,
		StatusCode: resp.StatusCode,
		Size:       n,
		Error:      "",
	}
}

func main() {
	// List of URLs to scrape (you can change / extend this for your demo)
	urls := []string{
		"https://example.com",
		"https://uottawa.ca",
		"https://golang.org",
		"https://github.com",
		"https://www.google.com",
	}

	const numWorkers = 5

	jobs := make(chan string)
	results := make(chan PageFetchResult)

	fmt.Println("Fetching URLs concurrently using worker pool...")

	// Start workers
	for w := 1; w <= numWorkers; w++ {
		go worker(w, jobs, results)
	}

	// Send  URLs into jobs channel (producer)
	go func() {
		for _, url := range urls {
			jobs <- url
		}
		close(jobs) // Important: tells workers there are no more jobs
	}()

	// Collect results
	completed := 0
	for res := range results {
		if res.Error != "" {
			fmt.Printf("%s | ERROR: %s\n", res.URL, res.Error)
		} else {
			fmt.Printf("%s | Status: %d | Size: %d bytes\n",
				res.URL, res.StatusCode, res.Size)
		}

		completed++
		if completed == len(urls) {
			close(results)
		}
	}

	fmt.Println("Scraping complete!")
}

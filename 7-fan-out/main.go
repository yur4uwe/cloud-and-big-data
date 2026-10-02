package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yur4uwe/cloud/7-fan-out/fanout"
)

type Job struct {
	ID    int
	Title string
}

type JobResult struct {
	JobID       int
	ProcessedBy string
	Duration    time.Duration
}

func main() {
	fmt.Println("=== Fan-Out Pattern Demo ===")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const numWorkers = 3
	const numJobs = 6

	// Simulated heavy worker function
	processJob := func(ctx context.Context, job Job) JobResult {
		start := time.Now()
		// Simulate varying work duration
		sleepDuration := time.Duration(100+job.ID*30) * time.Millisecond
		select {
		case <-ctx.Done():
			return JobResult{JobID: job.ID, ProcessedBy: "cancelled"}
		case <-time.After(sleepDuration):
		}

		return JobResult{
			JobID:       job.ID,
			ProcessedBy: "Worker",
			Duration:    time.Since(start),
		}
	}

	// 1. Initialize FanOut worker pool with 3 workers
	pool := fanout.FanOut(ctx, processJob, numWorkers)

	// 2. Start a consumer goroutine to aggregate results as they arrive
	var consumerWg sync.WaitGroup
	consumerWg.Go(func() {
		for res := range pool.Results() {
			fmt.Printf("[RESULT] Job %d finished in %v\n", res.JobID, res.Duration.Round(time.Millisecond))
		}
	})

	// 3. Submit jobs into the pool
	fmt.Printf("Submitting %d jobs to %d workers...\n", numJobs, numWorkers)
	for i := range numJobs {
		job := Job{ID: i + 1, Title: fmt.Sprintf("Image-Processing-%d", i+1)}
		fmt.Printf("[SUBMIT] Job %d: %s\n", job.ID, job.Title)
		pool.Submit(job)
	}

	// 4. Seal the pool to signal that no more jobs will be submitted
	fmt.Println("All jobs submitted. Sealing pool...")
	pool.Seal()

	// 5. Wait for all workers to finish processing and results channel to close
	pool.Wait()
	consumerWg.Wait()

	fmt.Println("=== All jobs completed successfully! Graceful shutdown complete. ===")
}

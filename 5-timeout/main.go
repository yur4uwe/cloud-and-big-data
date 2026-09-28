package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yur4uwe/cloud/5-timeout/timeout"
)

func main() {
	t := 5 * time.Second
	fmt.Printf("Quick-time test: Press [Enter] before %v passes!\n> ", t)

	start := time.Now()
	resCh, cancel := timeout.WithTimeout(func(ctx context.Context) error {
		reader := bufio.NewReader(os.Stdin)
		_, err := reader.ReadString('\n')
		return err
	}, t)
	defer cancel()

	err := <-resCh
	elapsed := time.Since(start).Round(time.Millisecond)

	if errors.Is(err, timeout.ErrTimeout) {
		fmt.Printf("\nTimed out! You took longer than %v.\n", t)
		return
	} else if err != nil {
		fmt.Printf("\nInput error: %v\n", err)
		return
	}

	fmt.Printf("Success! You responded in %v.\n", elapsed)
}

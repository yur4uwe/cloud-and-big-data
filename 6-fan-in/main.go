package main

import (
	"context"
	"fmt"
	"time"

	"github.com/yur4uwe/cloud/6-fan-in/fanin"
)

func main() {
	fmt.Println("=== Fan-In Pattern Demo ===")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s1 := fanin.NewProducer(ctx, func(p *fanin.Producer[string]) {
		for i := 1; i <= 3; i++ {
			p.Emit(fmt.Sprintf("Producer-1 message %d", i))
			time.Sleep(100 * time.Millisecond)
		}
	})

	s2 := fanin.NewProducer(ctx, func(p *fanin.Producer[string]) {
		for i := 1; i <= 3; i++ {
			p.Emit(fmt.Sprintf("Producer-2 message %d", i))
			time.Sleep(150 * time.Millisecond)
		}
	})

	merged := fanin.FanIn(ctx, s1, s2)

	for res := range merged {
		if res.Err != nil {
			fmt.Printf("[ERROR] %v\n", res.Err)
		} else {
			fmt.Printf("[RECEIVED] %s\n", res.Value)
		}
	}

	fmt.Println("All streams finished merging.")
}

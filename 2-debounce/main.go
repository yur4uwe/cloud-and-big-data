package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yur4uwe/cloud/2-debounce/debounce"
)

func main() {
	debounced, dispose := debounce.Debounce(func() {
		fmt.Println("hello")
	}, time.Second)
	defer dispose()

	s := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">")
		line, err := s.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		if strings.TrimSpace(line) == "exit" {
			break
		} else {
			debounced()
		}
	}

	fmt.Println("bye")
}

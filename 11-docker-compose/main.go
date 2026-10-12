package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := net.Dial("tcp", redisAddr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to connect to Redis at %s: %v\n", redisAddr, err), http.StatusServiceUnavailable)
			return
		}
		defer conn.Close()

		fmt.Fprintf(conn, "INCR hits\r\n")
		reader := bufio.NewReader(conn)
		line, err := reader.ReadString('\n')
		if err != nil {
			http.Error(w, fmt.Sprintf("Redis error: %v\n", err), http.StatusInternalServerError)
			return
		}

		count := strings.TrimSpace(strings.TrimPrefix(line, ":"))
		fmt.Fprintf(w, "Hello from Go container! Page views: %s\n", count)
	})

	log.Println("Server running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

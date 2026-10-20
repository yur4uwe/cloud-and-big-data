package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yur4uwe/cloud/9-sharding/sharding"
)

func printHelp() {
	fmt.Println("\nAvailable commands:")
	fmt.Println("  put <key> <val>       - Insert or update a key-value pair")
	fmt.Println("  get <key>             - Retrieve the value for a key")
	fmt.Println("  del <key>             - Delete a key from the storage")
	fmt.Println("  populate <count>      - Insert <count> random keys to view distribution")
	fmt.Println("  add                   - Split the most loaded shard and add a new shard")
	fmt.Println("  remove <idx>          - Remove shard at index <idx> (keys absorbed by successor)")
	fmt.Println("  move <idx> <pos>      - Relocate shard <idx> to a new token position <pos>")
	fmt.Println("  show                  - Display current tokens and ASCII distribution histogram")
	fmt.Println("  help                  - Print this help menu")
	fmt.Println("  exit / quit           - Exit the program")
	fmt.Println()
}

func main() {
	fmt.Println("==================================================")
	fmt.Println("       Consistent Hashing Sharded Storage Demo     ")
	fmt.Println("==================================================")

	initialShards := 4
	storage, err := sharding.NewStorage(initialShards)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating storage: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Initialized storage with %d shards.\n", initialShards)
	fmt.Print(storage.Visualize())
	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		fmt.Print("shard-cli> ")
		if !scanner.Scan() || scanner.Err() != nil {
			fmt.Println("Error reading input:", scanner.Err())
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit", "quit", "q":
			fmt.Println("Exiting demo. Goodbye!")
			return

		case "help", "h":
			printHelp()

		case "show":
			fmt.Println()
			fmt.Print(storage.Visualize())

		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <val>")
				continue
			}
			key := parts[1]
			val, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Printf("Invalid integer value: %q\n", parts[2])
				continue
			}

			hash := sharding.HashKey(key)
			storage.Put(key, val)
			fmt.Printf("Put key=%q val=%d (hash=%d)\n", key, val, hash)
			fmt.Print(storage.Visualize())

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			hash := sharding.HashKey(key)
			val, ok := storage.Get(key)
			if !ok {
				fmt.Printf("Key %q (hash=%d) not found.\n", key, hash)
			} else {
				fmt.Printf("Key %q (hash=%d) -> Value: %d\n", key, hash, val)
			}

		case "del", "delete":
			if len(parts) < 2 {
				fmt.Println("Usage: del <key>")
				continue
			}
			key := parts[1]
			hash := sharding.HashKey(key)
			storage.Delete(key)
			fmt.Printf("Deleted key %q (hash=%d).\n", key, hash)
			fmt.Print(storage.Visualize())

		case "populate":
			if len(parts) < 2 {
				fmt.Println("Usage: populate <count>")
				continue
			}
			count, err := strconv.Atoi(parts[1])
			if err != nil || count <= 0 {
				fmt.Println("Please provide a positive integer count.")
				continue
			}

			fmt.Printf("Populating %d random keys...\n", count)
			for i := range count {
				k := fmt.Sprintf("auto-key-%d-%d", i, rng.Int63n(1_000_000_000))
				storage.Put(k, i)
			}
			fmt.Printf("Done. Total keys in storage: %d\n", storage.TotalKeys())
			fmt.Print(storage.Visualize())

		case "add":
			fmt.Println("Adding shard (splitting the most loaded shard)...")
			if err := storage.AddShard(); err != nil {
				fmt.Printf("Error adding shard: %v\n", err)
			} else {
				fmt.Printf("Successfully added shard. Current shard count: %d\n", len(storage.Shards))
				fmt.Print(storage.Visualize())
			}

		case "remove", "rm":
			if len(parts) < 2 {
				fmt.Println("Usage: remove <shard_index>")
				continue
			}
			idx, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("Please provide a valid numeric shard index.")
				continue
			}

			if err := storage.RemoveShard(idx); err != nil {
				fmt.Printf("Error removing shard %d: %v\n", idx, err)
			} else {
				fmt.Printf("Successfully removed shard %d. Current shard count: %d\n", idx, len(storage.Shards))
				fmt.Print(storage.Visualize())
			}

		case "move":
			if len(parts) < 3 {
				fmt.Println("Usage: move <shard_index> <new_token_position_uint32>")
				continue
			}
			idx, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("Please provide a valid shard index.")
				continue
			}
			pos64, err := strconv.ParseUint(parts[2], 10, 32)
			if err != nil {
				fmt.Println("Please provide a valid uint32 token position.")
				continue
			}

			newPos := uint32(pos64)
			if err := storage.MoveShard(idx, newPos); err != nil {
				fmt.Printf("Error moving shard %d to %d: %v\n", idx, newPos, err)
			} else {
				fmt.Printf("Successfully moved shard %d to token %d.\n", idx, newPos)
				fmt.Print(storage.Visualize())
			}

		default:
			fmt.Printf("Unknown command: %q. Type 'help' for available commands.\n", cmd)
		}
	}
}

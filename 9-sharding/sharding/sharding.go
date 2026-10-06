// Package sharding provides sharding algorithms for data processing.
package sharding

import (
	"fmt"
	"hash/crc32"
	"math"
	"slices"
)

type StorageItem struct {
	RingPositon uint32
	Value       int
}

type Shard = []StorageItem

type Storage struct {
	Tokens []uint32
	Shards []Shard
}

func NewStorage(shardCount int) (*Storage, error) {
	if shardCount < 1 {
		return nil, fmt.Errorf("shardCount must be greater than 0")
	}

	tokens := make([]uint32, shardCount)
	step := math.MaxUint32 / uint32(shardCount)
	for i := range shardCount {
		tokens[i] = uint32(i+1) * step
	}
	return &Storage{
		Tokens: tokens,
		Shards: make([]Shard, shardCount),
	}, nil
}

func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (s *Storage) Get(key string) (int, bool) {
	ringPos := hashKey(key)
	shardIndex, _ := slices.BinarySearch(s.Tokens, ringPos)
	if shardIndex == len(s.Tokens) {
		shardIndex = 0
	}
	for _, item := range s.Shards[shardIndex] {
		if item.RingPositon == ringPos {
			return item.Value, true
		}
	}
	return -1, false
}

func (s *Storage) Put(key string, value int) {
	ringPos := hashKey(key)
	shardIndex, _ := slices.BinarySearch(s.Tokens, ringPos)
	if shardIndex == len(s.Tokens) {
		shardIndex = 0
	}
	s.Shards[shardIndex] = append(s.Shards[shardIndex], StorageItem{
		RingPositon: ringPos,
		Value:       value,
	})
}

func (s *Storage) Delete(key string) {
	ringPos := hashKey(key)
	shardIndex, _ := slices.BinarySearch(s.Tokens, ringPos)
	if shardIndex == len(s.Tokens) {
		shardIndex = 0
	}

	s.Shards[shardIndex] = slices.DeleteFunc(s.Shards[shardIndex], func(item StorageItem) bool {
		return item.RingPositon == ringPos
	})
}

func (s *Storage) AddShard() error {
	maxLen := -1
	maxLenShard := -1
	for i := range s.Shards {
		shardLen := len(s.Shards[i])
		if shardLen > maxLen {
			maxLen = shardLen
			maxLenShard = i
		}
	}

	ringSpanEnd := s.Tokens[maxLenShard]
	ringSpanStart := uint32(0)
	if maxLenShard != 0 {
		ringSpanStart = s.Tokens[maxLenShard-1]
	} else {
		// Select the last shard as the starting point
		// When we must divide 0-th shard
		// to account for the wrap-around
		ringSpanStart = s.Tokens[len(s.Tokens)-1]
	}

	ringSpanMiddle := ringSpanStart + (ringSpanEnd-ringSpanStart)/2

	return s.addShardAt(ringSpanMiddle)
}

func (s *Storage) addShardAt(token uint32) error {
	insertIdx, found := slices.BinarySearch(s.Tokens, token)
	if found {
		return fmt.Errorf("addShardAt: token %d already exists", token)
	}

	victimIdx := insertIdx
	if insertIdx == len(s.Tokens) {
		victimIdx = 0
	}

	var predecessor uint32
	if insertIdx == 0 {
		predecessor = s.Tokens[len(s.Tokens)-1]
	} else {
		predecessor = s.Tokens[insertIdx-1]
	}

	newTokens := slices.Insert(s.Tokens, insertIdx, token)

	slicedShard := s.Shards[victimIdx]
	shardLen := len(slicedShard)
	newShard := make([]StorageItem, 0, shardLen/2+1)
	newSlicedShard := make([]StorageItem, 0, shardLen/2+1)

	for _, item := range slicedShard {
		if item.RingPositon-predecessor <= token-predecessor {
			newShard = append(newShard, item)
		} else {
			newSlicedShard = append(newSlicedShard, item)
		}
	}

	var newShards []Shard
	if insertIdx == len(s.Tokens) {
		newShards = slices.Clone(s.Shards)
		newShards[0] = newSlicedShard
		newShards = append(newShards, newShard)
	} else {
		newShards = slices.Replace(s.Shards, insertIdx, insertIdx+1, newShard, newSlicedShard)
	}

	if len(newShards) != len(newTokens) {
		panic(fmt.Sprintf("s.Shards and s.Tokens desynchronized after AddShard: %d != %d", len(newShards), len(newTokens)))
	}

	s.Shards = newShards
	s.Tokens = newTokens

	return nil
}

func (s *Storage) RemoveShard(idx int) error {
	if idx < 0 || idx >= len(s.Shards) {
		return fmt.Errorf("RemoveShard: index out of range: %d", idx)
	} else if len(s.Shards) == 1 {
		return fmt.Errorf("RemoveShard: cannot remove the last shard")
	}

	successorIdx := (idx + 1) % len(s.Shards)

	s.Shards[successorIdx] = append(s.Shards[successorIdx], s.Shards[idx]...)
	s.Shards = slices.Delete(s.Shards, idx, idx+1)
	s.Tokens = slices.Delete(s.Tokens, idx, idx+1)

	return nil
}

func (s *Storage) MoveShard(idx int, newPosition uint32) error {
	if idx < 0 || idx >= len(s.Shards) {
		return fmt.Errorf("MoveShard: index out of range: %d", idx)
	} else if len(s.Shards) == 1 {
		return fmt.Errorf("MoveShard: cannot remove the last shard")
	}

	insertIdx, found := slices.BinarySearch(s.Tokens, newPosition)
	if found {
		return fmt.Errorf("MoveShard: newPosition(%d) is already in use by %d", newPosition, insertIdx)
	}

	if err := s.RemoveShard(idx); err != nil {
		return err
	}
	return s.addShardAt(newPosition)
}

package sharding_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/yur4uwe/cloud/9-sharding/sharding"
)

func randomKey(rng *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}

func TestStorage_PutGetDelete(t *testing.T) {
	s, err := sharding.NewStorage(4)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	// Missing key
	val, ok := s.Get("nonexistent")
	if ok || val != -1 {
		t.Fatalf("expected missing key to return (-1, false), got (%d, %v)", val, ok)
	}

	// Put and Get
	testData := map[string]int{
		"user:101": 100,
		"user:102": 200,
		"user:103": 300,
		"user:104": 400,
		"user:105": 500,
	}

	for k, v := range testData {
		s.Put(k, v)
	}

	for k, expected := range testData {
		v, ok := s.Get(k)
		if !ok {
			t.Fatalf("expected key %q to exist", k)
		}
		if v != expected {
			t.Fatalf("expected value %d for key %q, got %d", expected, k, v)
		}
	}

	// Overwrite existing key
	s.Put("user:101", 999)
	v, ok := s.Get("user:101")
	if !ok || v != 999 {
		t.Fatalf("expected overwritten value 999 for key %q, got %d (ok: %v)", "user:101", v, ok)
	}

	s.Delete("user:101")
	_, ok = s.Get("user:101")
	if ok {
		t.Fatalf("expected deleted key %q to not exist", "user:101")
	}

	// Other keys still intact
	v, ok = s.Get("user:102")
	if !ok || v != 200 {
		t.Fatalf("expected key user:102 to still exist with value 200, got %d (ok: %v)", v, ok)
	}
}

func TestStorage_DistributionUniformity(t *testing.T) {
	shardCount := 5
	s, err := sharding.NewStorage(shardCount)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	totalKeys := 20000
	rng := rand.New(rand.NewSource(42))

	for i := range totalKeys {
		key := fmt.Sprintf("random-key-%d-%s", i, randomKey(rng, 10))
		s.Put(key, i)
	}

	if s.TotalKeys() != totalKeys {
		t.Fatalf("expected total keys %d, got %d", totalKeys, s.TotalKeys())
	}

	dist := s.Distribution()
	t.Logf("\n%s", s.Visualize())

	expectedPerShard := float64(totalKeys) / float64(shardCount)
	varianceSum := 0.0

	for i := range shardCount {
		count := float64(dist[i])
		if count == 0 {
			t.Fatalf("shard %d is empty, distribution is severely skewed", i)
		}
		diff := count - expectedPerShard
		varianceSum += diff * diff
	}

	stdDev := math.Sqrt(varianceSum / float64(shardCount))
	coeffOfVariation := stdDev / expectedPerShard

	t.Logf("Expected per shard: %.0f", expectedPerShard)
	t.Logf("Standard deviation: %.2f", stdDev)
	t.Logf("Coefficient of variation (CV = stdDev / mean): %.4f (%.2f%%)", coeffOfVariation, coeffOfVariation*100)

	// In consistent hashing with uniformly spaced tokens, CV should be small (< 10-15%)
	const maxAllowedCV = 0.15
	if coeffOfVariation > maxAllowedCV {
		t.Fatalf("distribution too skewed: coefficient of variation %.4f exceeds maximum allowed %.4f",
			coeffOfVariation, maxAllowedCV)
	}
}

// Helper to find which shard currently holds a given key's value
func findKeyShard(s *sharding.Storage, expectedVal int) int {
	for i, shard := range s.Shards {
		for _, item := range shard {
			if item.Value == expectedVal {
				return i
			}
		}
	}
	return -1
}

func TestStorage_StabilityOnAddShard(t *testing.T) {
	initialShards := 4
	s, err := sharding.NewStorage(initialShards)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	totalKeys := 4000
	keys := make([]string, totalKeys)
	rng := rand.New(rand.NewSource(123))

	for i := range totalKeys {
		keys[i] = fmt.Sprintf("key-%d-%s", i, randomKey(rng, 8))
		s.Put(keys[i], i)
	}

	// Record initial shard for each key
	initialShardOfKey := make([]int, totalKeys)
	for i, k := range keys {
		initialShardOfKey[i] = findKeyShard(s, i)
		if initialShardOfKey[i] == -1 {
			t.Fatalf("key %q not found before adding shard", k)
		}
	}

	t.Logf("Before AddShard:\n%s", s.Visualize())

	if err := s.AddShard(); err != nil {
		t.Fatalf("AddShard failed: %v", err)
	}

	t.Logf("After AddShard:\n%s", s.Visualize())

	if len(s.Shards) != initialShards+1 {
		t.Fatalf("expected %d shards, got %d", initialShards+1, len(s.Shards))
	}

	// 1. Correctness check: all keys must STILL be retrievable with correct values
	for i, k := range keys {
		val, ok := s.Get(k)
		if !ok {
			t.Fatalf("key %q lost after AddShard", k)
		}
		if val != i {
			t.Fatalf("key %q returned wrong value %d after AddShard, expected %d", k, val, i)
		}
	}

	// count how many keys were remapped
	remappedCount := 0
	for i := range keys {
		newShard := findKeyShard(s, i)
		if newShard != initialShardOfKey[i] {
			remappedCount++
		}
	}

	remappedFraction := float64(remappedCount) / float64(totalKeys)
	t.Logf("AddShard: %d / %d keys remapped (%.2f%%)", remappedCount, totalKeys, remappedFraction*100)

	// For consistent hashing going from 4 to 5 shards:
	// Theoretical remapping is ~ 1/(N+1) = ~ 20-30%.
	// With simple modulo hashing, it would be ~ 80%.
	// We verify remappedFraction is substantially < 100% (and close to ~ 25%).
	if remappedFraction > 0.45 {
		t.Fatalf("too many keys remapped on AddShard: %.2f%% (expected < 45%%)", remappedFraction*100)
	}
	if remappedFraction == 0 {
		t.Fatalf("expected some keys to be remapped to the new shard, got 0")
	}
}

// Stability on removing a shard: fraction of remapped keys substantially < 100%
func TestStorage_StabilityOnRemoveShard(t *testing.T) {
	initialShards := 5
	s, err := sharding.NewStorage(initialShards)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	totalKeys := 5000
	keys := make([]string, totalKeys)
	rng := rand.New(rand.NewSource(456))

	for i := range totalKeys {
		keys[i] = fmt.Sprintf("key-rem-%d-%s", i, randomKey(rng, 8))
		s.Put(keys[i], i)
	}

	// Identify which shard holds each key initially
	initialShardOfKey := make([]int, totalKeys)
	for i, k := range keys {
		initialShardOfKey[i] = findKeyShard(s, i)
		if initialShardOfKey[i] == -1 {
			t.Fatalf("key %q not found before removing shard", k)
		}
	}

	t.Logf("Before RemoveShard:\n%s", s.Visualize())

	// Remove shard 2
	victimIdx := 2
	victimKeyCount := len(s.Shards[victimIdx])
	if err := s.RemoveShard(victimIdx); err != nil {
		t.Fatalf("RemoveShard failed: %v", err)
	}

	t.Logf("After RemoveShard:\n%s", s.Visualize())

	if len(s.Shards) != initialShards-1 {
		t.Fatalf("expected %d shards, got %d", initialShards-1, len(s.Shards))
	}

	// Correctness check: all keys must STILL be retrievable
	for i, k := range keys {
		val, ok := s.Get(k)
		if !ok {
			t.Fatalf("key %q lost after RemoveShard", k)
		}
		if val != i {
			t.Fatalf("key %q returned wrong value %d after RemoveShard, expected %d", k, val, i)
		}
	}

	// Stability check: count how many keys moved to a DIFFERENT logical shard
	// Note: shards with index > victimIdx shifted left by 1.
	// Only keys originally in victimIdx should have been absorbed by another shard!
	keysFromVictimRemapped := 0
	keysFromOtherShardsMoved := 0

	for i := range keys {
		oldShard := initialShardOfKey[i]
		newShard := findKeyShard(s, i)

		if oldShard == victimIdx {
			// Keys from victim were absorbed by successor (which shifted to index victimIdx)
			keysFromVictimRemapped++
		} else {
			// Keys from non-victim shards should retain their relative position
			expectedNewShard := oldShard
			if oldShard > victimIdx {
				expectedNewShard = oldShard - 1
			}
			if newShard != expectedNewShard {
				keysFromOtherShardsMoved++
			}
		}
	}

	t.Logf("Victim shard had %d keys, absorbed: %d", victimKeyCount, keysFromVictimRemapped)
	t.Logf("Non-victim keys that unexpectedly moved: %d", keysFromOtherShardsMoved)

	if keysFromOtherShardsMoved > 0 {
		t.Fatalf("expected 0 non-victim keys to move, but %d moved", keysFromOtherShardsMoved)
	}

	remappedFraction := float64(keysFromVictimRemapped) / float64(totalKeys)
	t.Logf("RemoveShard: remapped fraction: %.2f%%", remappedFraction*100)

	// Roughly ~ 1/N = 20%
	if remappedFraction > 0.35 {
		t.Fatalf("too many keys remapped: %.2f%% (expected < 35%%)", remappedFraction*100)
	}
}

// Stability and correctness of MoveShard
func TestStorage_MoveShard(t *testing.T) {
	s, err := sharding.NewStorage(4)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	totalKeys := 4000
	keys := make([]string, totalKeys)
	rng := rand.New(rand.NewSource(789))

	for i := range totalKeys {
		keys[i] = fmt.Sprintf("move-key-%d-%s", i, randomKey(rng, 8))
		s.Put(keys[i], i)
	}

	t.Logf("Before MoveShard:\n%s", s.Visualize())

	// Pick a new position between tokens[0] and tokens[1]
	newPos := s.Tokens[0] + (s.Tokens[1]-s.Tokens[0])/3
	if err := s.MoveShard(2, newPos); err != nil {
		t.Fatalf("MoveShard failed: %v", err)
	}

	t.Logf("After MoveShard:\n%s", s.Visualize())

	// Correctness check: no keys lost
	for i, k := range keys {
		val, ok := s.Get(k)
		if !ok {
			t.Fatalf("key %q lost after MoveShard", k)
		}
		if val != i {
			t.Fatalf("key %q returned wrong value %d after MoveShard, expected %d", k, val, i)
		}
	}
}

// 6. Test removing the last shard (which wraps around to shard 0)
func TestStorage_RemoveLastShardWrapAround(t *testing.T) {
	s, err := sharding.NewStorage(4)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}

	totalKeys := 2000
	keys := make([]string, totalKeys)
	rng := rand.New(rand.NewSource(999))

	for i := range totalKeys {
		keys[i] = fmt.Sprintf("last-shard-key-%d-%s", i, randomKey(rng, 8))
		s.Put(keys[i], i)
	}

	keysInLastShard := len(s.Shards[len(s.Shards)-1])
	keysInFirstShardBefore := len(s.Shards[0])

	// Remove the last shard (index 3)
	lastIdx := len(s.Shards) - 1
	if err := s.RemoveShard(lastIdx); err != nil {
		t.Fatalf("RemoveShard(last) failed: %v", err)
	}

	if len(s.Shards) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(s.Shards))
	}

	keysInFirstShardAfter := len(s.Shards[0])
	if keysInFirstShardAfter != keysInFirstShardBefore+keysInLastShard {
		t.Fatalf("expected %d keys in first shard, got %d", keysInFirstShardBefore-keysInLastShard, keysInFirstShardAfter)
	}
}

// 7. Error handling and boundaries
func TestStorage_EdgeCasesAndErrors(t *testing.T) {
	_, err := sharding.NewStorage(0)
	if err == nil {
		t.Fatalf("expected error creating storage with 0 shards")
	}

	s, err := sharding.NewStorage(1)
	if err != nil {
		t.Fatalf("NewStorage(1) failed: %v", err)
	}

	// Cannot remove the only shard
	if err := s.RemoveShard(0); err == nil {
		t.Fatalf("expected error removing the only remaining shard")
	}

	// Cannot move out of range
	if err := s.MoveShard(-1, 100); err == nil {
		t.Fatalf("expected error moving index -1")
	}
	if err := s.MoveShard(5, 100); err == nil {
		t.Fatalf("expected error moving index 5")
	}

	// Duplicate token
	if err := s.MoveShard(0, s.Tokens[0]); err == nil {
		t.Fatalf("expected error moving to an existing token position")
	}
}

package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 1. Тест віконного обмеження з task.md:
// "Віконне обмеження: при 10 викликах за 1с і ліміті 3/с — виконується лише дозволена кількість."
func TestThrottle_WindowLimit_DropExcess(t *testing.T) {
	var count atomic.Int32

	// Ліміт: 3 виклики/с (capacity: 3, refillRate: 3/с, ModeDrop, Leading: true)
	throttled, dispose := throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   3,
		RefillRate: 3,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   false,
	})
	defer dispose()

	// Робимо 10 швидких викликів протягом короткого проміжку часу (~90мс << 1с)
	for range 10 {
		throttled()
		time.Sleep(10 * time.Millisecond)
	}

	// Повинно виконатися рівно 3 (початкова місткість), а решта 7 відкинута
	if got := count.Load(); got != 3 {
		t.Fatalf("expected exactly 3 calls to execute immediately from burst of 10, got: %d", got)
	}
}

// 2. Тести режимів Leading / Trailing з task.md:
// "Режими leading/trailing."

func TestThrottle_LeadingOnly(t *testing.T) {
	var count atomic.Int32

	// capacity: 1, refillRate: 10/с (інтервал 100ms)
	throttled, dispose := throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   false,
	})
	defer dispose()

	// 1. Перший виклик спрацьовує негайно (leading)
	throttled()
	if got := count.Load(); got != 1 {
		t.Fatalf("expected immediate execution on leading=true, got: %d", got)
	}

	// 2. Повторні виклики під час відсутності токенів відкидаються
	for range 5 {
		throttled()
		time.Sleep(5 * time.Millisecond)
	}
	if got := count.Load(); got != 1 {
		t.Fatalf("expected count to remain 1 when trailing=false, got: %d", got)
	}

	// 3. Чекаємо закінчення інтервалу (100ms) — trailing не повинен викликатися
	time.Sleep(120 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("expected count still 1 after interval, got: %d", got)
	}

	// 4. Наступний виклик після поповнення токена знову виконується негайно
	throttled()
	if got := count.Load(); got != 2 {
		t.Fatalf("expected count 2 after refill, got: %d", got)
	}
}

func TestThrottle_TrailingOnly(t *testing.T) {
	var count atomic.Int32

	// capacity: 1, refillRate: 10/с (інтервал 100ms), leading: false, trailing: true
	throttled, dispose := throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    false,
		Trailing:   true,
	})
	defer dispose()

	// 1. Перший виклик НЕ виконується негайно (leading: false)
	throttled()
	if got := count.Load(); got != 0 {
		t.Fatalf("expected 0 calls immediately with leading=false, got: %d", got)
	}

	// 2. Додаємо виклики всередині інтервалу
	for range 3 {
		throttled()
		time.Sleep(5 * time.Millisecond)
	}

	if got := count.Load(); got != 0 {
		t.Fatalf("expected 0 calls before trailing timer triggers, got: %d", got)
	}

	// 3. Чекаємо закінчення інтервалу: trailing повинен виконатися рівно 1 раз
	time.Sleep(150 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("expected exactly 1 trailing call, got: %d", got)
	}
}

func TestThrottle_LeadingAndTrailing_Burst(t *testing.T) {
	var count atomic.Int32

	// capacity: 1, refillRate: 10/с (інтервал 100ms), leading: true, trailing: true
	throttled, dispose := throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   true,
	})
	defer dispose()

	// Перший виклик спрацьовує негайно (leading)
	throttled()
	if got := count.Load(); got != 1 {
		t.Fatalf("expected immediate leading call, got: %d", got)
	}

	// Серія викликів під час заблокованого періоду
	for range 4 {
		throttled()
		time.Sleep(10 * time.Millisecond)
	}

	// Поки інтервал не сплив, виконався лише 1 виклик
	if got := count.Load(); got != 1 {
		t.Fatalf("expected count 1 before interval expiry, got: %d", got)
	}

	// Чекаємо завершення інтервалу
	time.Sleep(150 * time.Millisecond)

	// Має бути 2 виклики: 1 leading + 1 trailing
	if got := count.Load(); got != 2 {
		t.Fatalf("expected exactly 2 executions (1 leading + 1 trailing), got: %d", got)
	}
}

// 3. Тести черги та відсікання з task.md:
// "Черга: порядок збережено; відсікання: зайві відкинуто."

func TestThrottle_DropExcess(t *testing.T) {
	var count atomic.Int32

	// capacity: 2, refillRate: 10/с, ModeDrop
	throttled, dispose := throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   2,
		RefillRate: 10,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   false,
	})
	defer dispose()

	var accepted int
	for range 10 {
		if throttled() {
			accepted++
		}
	}

	// З 10 швидких викликів лише 2 мають бути прийняті, а 8 відкинуті
	if accepted != 2 {
		t.Fatalf("expected 2 accepted calls, got: %d", accepted)
	}
	if got := count.Load(); got != 2 {
		t.Fatalf("expected count to be 2, got: %d", got)
	}
}

func TestThrottle_Queue_PreservesOrder(t *testing.T) {
	var (
		mu      sync.Mutex
		results []int
	)

	// Refill 20/sec (кожні ~50ms обробляється завдання), ModeQueue
	opts := ThrottleOpts{
		Capacity:   1,
		RefillRate: 20,
		Mode:       ModeQueue,
	}

	tb := NewTokenBucket(opts.Capacity, opts.RefillRate)
	queue := make(chan int, 20)
	var wg sync.WaitGroup

	// Споживач черги, який перевіряє Token Bucket та записує порядок
	go func() {
		for val := range queue {
			_ = tb.Wait(t.Context(), 1)
			mu.Lock()
			results = append(results, val)
			mu.Unlock()
			wg.Done()
		}
	}()

	// Відправляємо 5 завдань у чергу з конкретними порядковими номерами
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		queue <- i
	}

	wg.Wait()
	close(queue)

	// Перевіряємо порядок FIFO: [1, 2, 3, 4, 5]
	expected := []int{1, 2, 3, 4, 5}
	mu.Lock()
	defer mu.Unlock()

	if len(results) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(results))
	}
	for i, val := range expected {
		if results[i] != val {
			t.Fatalf("expected result at index %d to be %d, got %d (order not preserved)", i, val, results[i])
		}
	}
}

func TestThrottle_Dispose_StopsTrailingExecution(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   1,
		RefillRate: 10, // інтервал 100ms
		Mode:       ModeDrop,
		Leading:    false,
		Trailing:   true,
	})

	throttled() // планує trailing виклик через 100ms

	// Викликаємо dispose через 20ms до спрацьовування таймера
	time.Sleep(20 * time.Millisecond)
	dispose()

	// Чекаємо більше ніж інтервал
	time.Sleep(150 * time.Millisecond)

	if got := count.Load(); got != 0 {
		t.Fatalf("expected 0 calls because dispose canceled trailing timer, got: %d", got)
	}
}

func TestThrottle_ConcurrentSafety(t *testing.T) {
	var count atomic.Int32

	throttled, dispose := throttle(func() {
		count.Add(1)
	}, ThrottleOpts{
		Capacity:   5,
		RefillRate: 50,
		Mode:       ModeDrop,
		Leading:    true,
		Trailing:   true,
	})
	defer dispose()

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range 20 {
				throttled()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	if got := count.Load(); got == 0 {
		t.Fatalf("expected count > 0 under concurrent load, got: %d", got)
	}
}

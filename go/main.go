package main

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const N = 1_000_000_000

func sumSeq(a []int8) int64 {
	var s int64
	for _, x := range a {
		s += int64(x)
	}
	return s
}

func bounds(n, parts, p int) (int, int) {
	chunk := n / parts
	from := p * chunk
	to := from + chunk
	if p == parts-1 {
		to = n
	}
	return from, to
}


func sumWG(a []int8, parts int) int64 {
	res := make([]int64, parts)
	var wg sync.WaitGroup
	for p := 0; p < parts; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			from, to := bounds(len(a), parts, p)
			res[p] = sumSeq(a[from:to])
		}(p)
	}
	wg.Wait()
	var total int64
	for _, r := range res {
		total += r
	}
	return total
}

func sumChan(a []int8, parts int) int64 {
	ch := make(chan int64, parts)
	for p := 0; p < parts; p++ {
		go func(p int) {
			from, to := bounds(len(a), parts, p)
			ch <- sumSeq(a[from:to])
		}(p)
	}
	var total int64
	for i := 0; i < parts; i++ {
		total += <-ch
	}
	return total
}


func sumMutex(a []int8, parts int) int64 {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var total int64
	for p := 0; p < parts; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			from, to := bounds(len(a), parts, p)
			local := sumSeq(a[from:to])
			mu.Lock()
			total += local
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	return total
}


func sumAtomicBad(a []int8, parts int) int64 {
	var total atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < parts; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			from, to := bounds(len(a), parts, p)
			for _, x := range a[from:to] {
				total.Add(int64(x))
			}
		}(p)
	}
	wg.Wait()
	return total.Load()
}

func bench(name string, f func() int64) {
	const runs = 5
	ts := make([]int64, runs)
	var r int64
	for i := 0; i < runs; i++ {
		t := time.Now()
		r = f()
		ts[i] = time.Since(t).Milliseconds()
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	fmt.Printf("%-16s sum=%d  median=%d ms  min=%d ms\n", name, r, ts[runs/2], ts[0])
}

func main() {
	a := make([]int8, N)
	for i := range a {
		a[i] = int8(i % 100)
	}

	bench("sequential", func() int64 { return sumSeq(a) })

	for _, parts := range []int{1, 2, 4, runtime.NumCPU(), runtime.NumCPU() * 2} {
		fmt.Println("--- parts =", parts)
		bench("waitgroup", func() int64 { return sumWG(a, parts) })
		bench("channels", func() int64 { return sumChan(a, parts) })
		bench("mutex", func() int64 { return sumMutex(a, parts) })
	}

	small := a[:50_000_000]
	fmt.Println("--- atomic (bad), 50M elements")
	bench("seq (50M)", func() int64 { return sumSeq(small) })
	bench("atomic/element", func() int64 { return sumAtomicBad(small, runtime.NumCPU()) })
}

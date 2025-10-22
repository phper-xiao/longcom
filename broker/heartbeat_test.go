package broker

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestPriorityQueue(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	q := NewPriorityQueue()
	for i := 0; i < 100; i++ {
		n := rand.Int63n(100)
		q.Push(&PQItem{
			Priority: n,
		})
	}
	for _, item := range *q.pqimpl {
		fmt.Printf("%d, ", item.Priority)
	}

	fmt.Printf("\n%d\n", q.Pop().Priority)
}

package main

import (
	"fmt"
	datastructures "go-snippets/data-structures"
)

func main() {
	// TESTING INT STACK
	intStack := datastructures.NewStack[int]()
	intStack.Push(10)
	intStack.Push(20)
	intStack.Push(30)

	fmt.Println("Int Stack Size:", intStack.Size()) // Should print 3

	top, ok := intStack.Pop()
	fmt.Printf("Popped: %d, OK: %t\n", top, ok) // Should print 30, true

	peek, ok := intStack.Peek()
	fmt.Printf("Peek: %d, OK: %t\n", peek, ok) // Should print 20, true

	intStack.Reverse()
	peek, ok = intStack.Peek()
	fmt.Println("After Reverse, Top:", peek) // Should print 10 (since stack was [10,20], reversed -> [20,10], top is 10)

	fmt.Println("Is empty? ", intStack.IsEmpty())
}

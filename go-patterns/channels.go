package gopatterns

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Workers Pool

func WorkersPool() {
	var tasks []string
	for i := 0; i < 20; i++ {
		tasks = append(tasks, fmt.Sprintf("Task %d", i+1))
	}
	createPool(tasks, 5)
}

func createPool(tasks []string, workersNum int) {
	taskCh := make(chan string, len(tasks))
	var wg sync.WaitGroup

	// start workers
	for i := 0; i < workersNum; i++ {
		wg.Go(func() {
			// for {
			// 	select {
			// 	case val, ok := <-taskCh:
			// 		if !ok {
			// 			fmt.Println("Channel is closed and empty!")
			// 			return
			// 		}
			// 		fmt.Printf("Received a value: %s, worker #%d\n", val, i)
			// 	}
			// }

			// range on channel handles "is channel closed" check automatically
			for task := range taskCh {
				fmt.Printf("Received a value: %s, worker #%d\n", task, i)
			}
		})
	}

	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)
	wg.Wait()

	fmt.Println("Completed!")
}

// Three-Stage Pipeline
func CreatePipeline() {
	ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
	itemsToSquare := 10
	squareCh := generator(ctx, itemsToSquare)
	resCh := square(ctx, squareCh)
	printer(ctx, resCh)

}

// generator: Sends numbers to count into the returned channel.
func generator(ctx context.Context, count int) <-chan int {
	squareCh := make(chan int)
	go func() {
		defer close(squareCh)
		for i := 0; i < count; i++ {
			select {
			case squareCh <- i + 1:
			case <-ctx.Done():
				fmt.Println("generator cancelled")
				return
			}
		}
	}()
	return squareCh
}

// square: Reads from the input channel, squares each number, and sends it to the output channel.
func square(ctx context.Context, in <-chan int) <-chan int {
	resCh := make(chan int)
	go func() {
		defer close(resCh)
		for {
			select {
			case val, ok := <-in:
				if !ok {
					fmt.Println("square: input channel closed")
					return
				}
				res := val * val
				select {
				case resCh <- res:
				case <-ctx.Done():
					fmt.Println("square cancelled")
				}

			case <-ctx.Done():
				fmt.Println("square cancelled")
				return
			}
		}
	}()
	return resCh
}

// printer: Reads from the input channel and prints each number.
func printer(ctx context.Context, in <-chan int) {
	for {
		select {
		case val, ok := <-in:
			if !ok {
				fmt.Println("printer: input channel closed")
				return
			}
			fmt.Println(val)
		case <-ctx.Done():
			fmt.Println("printer cancelled")
			return
		}
	}
}

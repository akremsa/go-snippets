package gopatterns

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// test blocked write in select
func TestBlockedSelect() {
	dataCh := make(chan int)
	quitCh := make(chan struct{})

	var wg sync.WaitGroup
	wg.Go(func() {
		// log.Print("Starting reader...")
		// for {
		// 	select {
		// 	case val := <-dataCh:
		// 		log.Printf("Reader recevied value: %d", val)
		// 	case <-quitCh:
		// 		log.Print("Shutting down the reader...")
		// 		return
		// 	}
		// }
		log.Print("Starting writer...")
		val := 3
		i := 0
		for {
			log.Printf("Writer iteration %d", i)
			i++
			select {
			case dataCh <- val:
				log.Printf("Writer sent the value: %d", val)
			case <-quitCh:
				log.Print("Shutting down the writer...")
				return
			}
		}
	})

	log.Print("Sleep...")
	time.Sleep(2 * time.Second)
	val := <-dataCh
	log.Printf("Recevied value: %d", val)
	log.Print("Sleep...")
	time.Sleep(2 * time.Second)
	log.Print("Close writer")
	close(quitCh)
	wg.Wait()
	log.Print("Exit")
}

// Workers Pool

func WorkersPool() {
	var tasks []string
	for i := range 20 {
		tasks = append(tasks, fmt.Sprintf("Task %d", i+1))
	}
	createPool(tasks, 5)
}

func createPool(tasks []string, workersNum int) {
	taskCh := make(chan string, len(tasks))
	var wg sync.WaitGroup

	// start workers
	for i := range workersNum {
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
		for i := range count {
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

// Worker pool

// Task represents a unit of work
type Task struct {
	ID   int
	Data string
}

// Result represents the outcome of a task
type Result struct {
	TaskID int
	Value  string
	Err    error
}

// WorkerPool manages the worker goroutines
type WorkerPool struct {
	taskCh   chan Task
	resultCh chan Result
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func StartWorkerPool() {
	workersNum := 5
	wp := NewWorkerPool(context.Background(), workersNum)

	go func() {
		for i := range 5 {
			task := Task{
				ID:   i,
				Data: fmt.Sprintf("Test data - %d", i),
			}
			wp.Submit(task)
		}
		wp.Shutdown()
	}()

	for res := range wp.resultCh {
		fmt.Printf("Received task %d, data: %s\n", res.TaskID, res.Value)
	}
}

// NewWorkerPool creates a new worker pool with the given number of workers
func NewWorkerPool(ctx context.Context, workersNum int) *WorkerPool {
	ctxCancellable, cancel := context.WithCancel(ctx)
	wp := &WorkerPool{
		taskCh:   make(chan Task, workersNum),
		resultCh: make(chan Result, workersNum),
		ctx:      ctxCancellable,
		cancel:   cancel,
	}

	for i := range workersNum {
		wp.wg.Go(func() {
			for {
				select {
				case t, ok := <-wp.taskCh:
					if !ok {
						fmt.Println("Task channel is closed, exiting...")
						return
					}
					fmt.Printf("Worker #%d: Processing taskID: %d, data: %s\n", i+1, t.ID, t.Data)
					res := processingTask(t)
					select {
					case wp.resultCh <- res:
					case <-wp.ctx.Done():
						fmt.Printf("Worker %d is about to stop...\n", i+1)
					}

				case <-wp.ctx.Done():
					fmt.Printf("Worker %d is about to stop...\n", i+1)
					return
				}
			}
		})
	}

	return wp
}

// Submit adds a task to the pool (non-blocking if buffer has space)
func (wp *WorkerPool) Submit(task Task) {
	select {
	case wp.taskCh <- task:
		fmt.Printf("Task %d submitted\n", task.ID)
	case <-wp.ctx.Done():
		fmt.Println("Submit cancelled...")
	}
}

// Results returns a channel for reading results
func (wp *WorkerPool) Results() <-chan Result {
	return wp.resultCh
}

// Shutdown gracefully shuts down the pool, waiting for all tasks to complete
func (wp *WorkerPool) Shutdown() {
	close(wp.taskCh)
	wp.wg.Wait()
	close(wp.resultCh)
	wp.cancel()
}

func processingTask(task Task) Result {
	time.Sleep(1 * time.Millisecond)
	return Result{
		TaskID: task.ID,
		Value:  task.Data,
	}
}

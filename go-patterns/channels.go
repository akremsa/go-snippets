package gopatterns

import (
	"fmt"
	"sync"
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

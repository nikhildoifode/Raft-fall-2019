package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type TaskStatus struct {
	taskNum   int
	taskState int
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	availWorkers := make(chan string, ntasks)
	availTasks := make(chan TaskStatus, ntasks)

	go func() {
		for address := range registerChan {
			availWorkers <- address
		}
	}()

	var wg sync.WaitGroup
	for index := 0; index < ntasks; index++ {
		availTasks <- TaskStatus{index, 0}
		wg.Add(1)
	}

	go func() {
		for task := range availTasks {
			var args DoTaskArgs
			if phase == mapPhase {
				args = DoTaskArgs{jobName, mapFiles[task.taskNum], phase, task.taskNum, n_other}
			} else {
				args = DoTaskArgs{jobName, "", phase, task.taskNum, n_other}
			}
			go func(tkCounter int, arg DoTaskArgs) {
				addr := <-availWorkers
				reply := call(addr, "Worker.DoTask", arg, nil)
				if reply == false {
					availTasks <- TaskStatus{tkCounter, 0}
				} else {
					availWorkers <- addr
					wg.Done()
				}
			}(task.taskNum, args)
		}
	}()

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

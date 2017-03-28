package mapreduce

import (
	"fmt"
    "sync"
    "strconv"
    "strings"
)


type stack struct {
     lock sync.Mutex // you don't have to do this if you don't want thread safety
     s []int
}

func NewStack() *stack {
    return &stack {sync.Mutex{}, make([]int,0), }
}

func (s *stack) Push(v int) {
    s.lock.Lock()
    defer s.lock.Unlock()

    s.s = append(s.s, v)
}

func (s *stack) Pop() (int) {
    s.lock.Lock()
    defer s.lock.Unlock()
    l := len(s.s)

    res := s.s[l-1]

    s.s = s.s[:l-1]

    return res
}

func (s *stack) Empty() bool {
	return (len(s.s)==0)
}


func (mr *Master) WorkerThread(u_fin_task int, argtemplate DoTaskArgs, 
	workername string, wg *sync.WaitGroup) {
	fmt.Printf("Worker: %s thread is scheduled to work\n", workername)
	next_task := argtemplate
	next_task.File = mr.files[u_fin_task]
	next_task.TaskNumber = (u_fin_task)
	ok := call(workername, "Worker.DoTask", next_task, new(struct{}))

	if(ok == false) {
		fmt.Printf("Worker: RPC %s DoTask error\n", workername)
		go func() {
			mr.registerChannel <- ("Failed " + strconv.Itoa(u_fin_task))
		}()
	}else {
		go func() {
			mr.registerChannel <- workername
		}()
	 	wg.Done()
	}

}




// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	taskarg := DoTaskArgs{mr.jobName, "", phase, 0, nios}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	failed_tasks_stack := NewStack()
	task := 0
	for{	
		register_response := <-mr.registerChannel
		if(strings.Contains(register_response, "Failed")) {
			tokens := strings.Split(register_response, " ")
			failed_task,_ := strconv.Atoi(tokens[1])
			failed_tasks_stack.Push(failed_task)
		} else  {
			if(failed_tasks_stack.Empty()) {
				wg.Add(1)
				go mr.WorkerThread(task, taskarg, register_response, &wg)
				task++			
				if(task>=ntasks) {
					break
				}
			}else {
				p_task := failed_tasks_stack.Pop()
				go mr.WorkerThread(p_task, taskarg, register_response, &wg)
			}
		}
	}	
	wg.Wait()
	fmt.Printf("Schedule: %v tasks (%d I/Os) IS DONE!\n", phase, nios)
}

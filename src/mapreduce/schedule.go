package mapreduce

import "fmt"


func (mr *Master) WorkerThread(u_fin_task *int, fin_task *int, argtemplate DoTaskArgs, 
	workername string, ntasks int, scheduleFinish chan bool) {
	fmt.Printf("Worker: %s thread is scheduled to work\n", workername)
	for {
		mr.Lock()
		if( (*u_fin_task)>=ntasks ) {
			mr.Unlock()
			return
		}
		next_file_name := mr.files[*u_fin_task]
		(*u_fin_task)++
		mr.Unlock()

		next_task := argtemplate
		next_task.File = next_file_name
		next_task.TaskNumber = (*u_fin_task)
		ok := call(workername, "Worker.DoTask", next_task, new(struct{}))
		if(ok == false) {
			fmt.Printf("Worker: RPC %s DoTask error\n", workername)
		}

		mr.Lock()
		(*fin_task)++
		fmt.Printf("Worker: %s  has finished %d task\n", workername, (*fin_task))

		if( (*fin_task)>=ntasks ) {
			scheduleFinish <- true
			mr.Unlock()
			return
		}
		mr.Unlock()		
	}
}



// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	u_fin_task  := 0
	fin_task := 0
	quit := make(chan bool)
	for{
		select {
			case reg_chanel := <-mr.registerChannel:
				fmt.Printf("Schdule recive register: worker %s\n", reg_chanel)
			default:
				fmt.Printf("Schdule Registed %d workers\n", len(mr.workers))
				break
		}
		break
	}
	fmt.Printf("REGINTEATION PHASE DONE\n")
	numworkers := len(mr.workers)
	taskarg := DoTaskArgs{mr.jobName, "", phase, 0, nios}
	for i := 0; i < numworkers; i++ {
		go mr.WorkerThread(&u_fin_task, &fin_task, taskarg, 
			mr.workers[i], ntasks, quit)
	}

	if(<-quit) {
		fmt.Printf("Schedule: %v phase done\n", phase)
		return
	}

}

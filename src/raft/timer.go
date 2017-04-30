package raft

import (
	"time"
	"math/rand"
	"sync"
	//"fmt"
)

type Timer struct {
	internalTimer *time.Timer
	minduration time.Duration
	maxduration time.Duration
	timePeriod 	time.Duration
	mutex         sync.Mutex
	rand          *rand.Rand

}

func (t *Timer) RandDuration() time.Duration {
	if(t.maxduration - t.minduration==0) {
		t.timePeriod = t.minduration
	}else {
		t.timePeriod = t.minduration + time.Duration( rand.Int63n( int64(t.maxduration - t.minduration) ) )
	}
	return t.timePeriod
}

func NewTimer(mintime time.Duration, maxtime time.Duration) *Timer {
	timer := &Timer {
		internalTimer : time.NewTimer(1),
		minduration : mintime,
		maxduration : maxtime,
	}
	return timer
}

func (t *Timer) GetMinDuration() time.Duration {
	return t.minduration
}

func (t *Timer) Ticking() time.Duration {
	return t.minduration
}


func (t *Timer) C() <- chan time.Time {
	return t.internalTimer.C
}

func (t *Timer) Pause() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return 	t.internalTimer.Stop() 
}


func (t *Timer) Reset() bool{
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.internalTimer.Stop() 
	//fmt.Println("resetting timer")
	return t.internalTimer.Reset(t.RandDuration())
}

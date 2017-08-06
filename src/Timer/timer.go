package timer

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
	mutex         sync.Mutex
	rand          *rand.Rand

}

func (t *Timer) RandDuration() time.Duration {
	timePeriod := t.minduration + time.Duration( rand.Int63n( int64(t.maxduration - t.minduration) ) )
	//fmt.Printf( "rand timeperiod %v\n", timePeriod )
	return timePeriod
}

func NewTimer(mintime time.Duration, maxtime time.Duration) *Timer {
	timer := &Timer {
		internalTimer : time.NewTimer(1),
		minduration : mintime,
		maxduration : maxtime,
	}
	timer.Reset()
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




func (t *Timer) Reset() bool{
	t.internalTimer.Stop() 
	//fmt.Println("resetting timer")
	return t.internalTimer.Reset(t.RandDuration())
}

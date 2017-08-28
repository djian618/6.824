package timer

import (
	//"sync"
	"testing"
	"time"
	"fmt"
)


func TestTimerInitalization(t *testing.T) {
	timer := NewTimer(200*time.Millisecond, 300*time.Millisecond)
	fmt.Printf("Initalize timer with min time %d\n", timer.GetMinDuration())
}

// this is for testing how many  ticking between some time period
func TestTimerTicking(t *testing.T) {
	timer := NewTimer(100*time.Millisecond, 200*time.Millisecond)
	count := 0
	breaker := make(chan bool, 1)
    //timer.Reset()
    go func() {
    	for {
    		//fmt.Println("waiting for timmer interrupt")
    		select {
    		case  <-timer.C():
    			count++
    			if(!timer.Reset()) {
    				//fmt.Println("timer reset return false")
    			}
    		case <-breaker:
    			break
    		}
    	}
    	return
    }()
   	time.Sleep(500 * time.Millisecond)
   	breaker<-true
   	if(count<2) {
   		t.Fatalf("Timer should have executed at least twice (%d)", count)
   	}
   	if(count>5) {
   		t.Fatalf("Timer should have executed at most five (%d)", count)
   	}

}


func TestReset(t *testing.T) {
    timer := NewTimer(100*time.Millisecond, 200*time.Millisecond)
    ticking := make(chan bool, 1)
    exit := make(chan bool, 1)
    go func() {
      for {
        time.Sleep(50 * time.Millisecond)
        ticking <- true
      }
    }()

    go func() {
        time.Sleep(2 * time.Second)
        exit <- true
    }()


    for {
      select {
        case <- ticking :
          timer.Reset()
        case <- timer.C() :
            t.Fatalf("timer exit regarless of the reset\n")
        case <- exit:
            return
      }
    }
}





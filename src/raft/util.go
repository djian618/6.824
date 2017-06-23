package raft

import "log"
import "os"
// Debugging
const Debug = 1
const Bebug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	if Debug ==2 {
		f, _ := os.OpenFile("testlogfile", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer f.Close()

		log.SetOutput(f)
		log.Printf(format, a...)

	}
	return
}

func BPrintf(format string, a ...interface{}) (n int, err error) {
	if Bebug == 1 {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

package raft

import (
    "log"
    "os"
)

// Debugging
const Debug = 0
const Bebug = 2
const fileName = "testlogfile1"

var openFileFlag bool = false


func LoggerInit()  *log.Logger {
	var file, _ = os.Create(fileName)
	//defer file.Close()
	logger := log.New(file, "raftLog: ", log.Ldate|log.Ltime)
	return logger
}

var globalLogger *log.Logger = LoggerInit()


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 1 {
		log.Printf(format, a...)
	}
	if Debug ==2 {
		globalLogger.Printf(format, a...)
	}
	return
}

func BPrintf(format string, a ...interface{}) (n int, err error) {
	if Bebug == 1 {
		log.Printf(format, a...)
	}

	if Bebug == 2 {
		globalLogger.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func CloseLogger() {
	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

}
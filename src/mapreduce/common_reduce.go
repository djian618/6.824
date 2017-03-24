package mapreduce
import (
	"fmt"
	"os"
	"encoding/json"
	"sort"
)
// this variable is used to test if the reduced words is the same as the mapped words
var g_total_reduced_words int = 0

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	kv_map := make(map[string][]string)
	
	for i := 0; i<nMap; i++ {
		file, err :=  os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			fmt.Print(err)
		}else {
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue 
				err := dec.Decode(&kv)
				if err != nil {
					break
				}
				_, ok := kv_map[kv.Key]
				if(!ok) {
					kv_map[kv.Key] = make([]string, 0)
				}
				kv_map[kv.Key] = append(kv_map[kv.Key], kv.Value)
			}
			file.Close()
		}
	}
	
	//keys := make([] string, len(kv_map) )
	var keys [] string
	for key,_ := range kv_map {
		keys = append(keys, key)
	}

	sort.Strings(keys)  // 递增排序
	g_total_reduced_words += len(keys)

	mergefile,_ := os.Create(mergeName(jobName, reduceTaskNumber))
	
	enc := json.NewEncoder(mergefile)

	for _,k := range keys {
		//fmt.Println(key)
		enc.Encode(KeyValue{k, reduceF(k, kv_map[k])})
	}
	mergefile.Close()
}

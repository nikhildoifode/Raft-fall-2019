package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	var kvPairs []KeyValue
	for m := 0; m < nMap; m++ {
		reduceFileName := reduceName(jobName, m, reduceTask)

		reduceFileP, err := os.Open(reduceFileName)
		if err != nil {
			log.Println(err)
		}

		decoder := json.NewDecoder(reduceFileP)
		for decoder.More() {
			var m KeyValue
			decoder.Decode(&m)
			kvPairs = append(kvPairs, m)
		}

		reduceFileP.Close()
	}

	sort.Slice(kvPairs, func(i, j int) bool {
		return kvPairs[i].Key < kvPairs[j].Key
	})

	outFileP, err := os.Create(outFile)
	if err != nil {
		log.Println(err)
	}

	encoder := json.NewEncoder(outFileP)

	var valuesList []string
	index := 0
	oldKey := kvPairs[0].Key
	lastKey := ""

	for index < len(kvPairs) {
		kv := kvPairs[index]

		if oldKey == kv.Key {
			valuesList = append(valuesList, kv.Value)
			index++
		} else {
			encoder.Encode(KeyValue{oldKey, reduceF(oldKey, valuesList)})
			valuesList = nil
			oldKey = kv.Key
			lastKey = kv.Key
		}
	}

	encoder.Encode(KeyValue{lastKey, reduceF(lastKey, valuesList)})

	outFileP.Close()
}

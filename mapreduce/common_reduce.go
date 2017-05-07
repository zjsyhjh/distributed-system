package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
	kvs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		//get the KeyValue of mrtmp-jobName-i-reduceTaskNumber, then put them into kvs
		//example : put the key of mrtmp.test-0-0, mrtmp.test-1-0, mrtmp.test-2-0 into outputfile(mrtmp-test-0)
		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fileName)
		defer file.Close()
		if err != nil {
			log.Fatal("doReduce: Open ", err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			// key -> {value1, value2, value3, ... }
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create(outFile)
	defer file.Close()
	if err != nil {
		log.Fatal("doReduce: Create ", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		//reduceF: (key, {value1, value2, ...}) -> (key, value)
		enc.Encode(KeyValue{k, reduceF(k, kvs[k])})
	}
}

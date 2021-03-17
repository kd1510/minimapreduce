package main

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"strings"
	"time"
	"unicode"
)

type Worker struct {
	RunningTask     bool
	CurrentTask     Task
	ReduceFilePaths map[string]int
}

func (w *Worker) requestTask() error {
	client, _ := rpc.DialHTTP("tcp", "localhost:1234")

	var task Task
	err := client.Call("Server.RequestTask", 0, &task)
	check(err)
	if task.PathToPartition != "" {
		w.CurrentTask = task
		w.RunningTask = true
	}
	return nil
}

func (w *Worker) sendReduceFilepaths() error {

	var filepaths []string
	for k, _ := range w.ReduceFilePaths {
		filepaths = append(filepaths, k)
	}

	client, _ := rpc.DialHTTP("tcp", "localhost:1234")
	var reply string
	err := client.Call("Server.RegisterReduceFiles", ReduceFiles{filepaths}, &reply)
	check(err)
	return nil
}

func (w *Worker) runTask(task Task) {
	if w.CurrentTask.TaskType == "Map" {
		fmt.Println("Starting processing partition for Map Task:", w.CurrentTask.PathToPartition)
		contents, err := ioutil.ReadFile(w.CurrentTask.PathToPartition)
		check(err)
		result := w.WordCountMap(string(contents))
		w.SaveToReduceFiles(result)
		w.sendReduceFilepaths()
	}
	if w.CurrentTask.TaskType == "Reduce" {

		//Read all files for task number
		//THIS WORK!!!!!!!!! just refactor it to make a clean API for user
		allKeys := make(map[string]int)
		for _, file := range w.CurrentTask.AllReducePartitionLocations {
			if strings.Contains(file, fmt.Sprintf("reduce_%v", w.CurrentTask.ReduceTaskNumber)) {
				contents, err := ioutil.ReadFile(file)
				check(err)
				lines := strings.Split(string(contents), "\n")
				for _, l := range lines {
					temp := strings.Split(l, ",")
					if len(temp) > 1 {
						array := strings.Split(strings.ReplaceAll(strings.ReplaceAll(temp[1], "[", ""), "]", ""), " ")
						allKeys[temp[0]] = allKeys[temp[0]] + len(array)
					}
				}
			}
		}

		fmt.Printf("RESULT OF TOTAL REDUCE FOR TASK %v:  %v",
			w.CurrentTask.ReduceTaskNumber, allKeys)

		//result := w.WordCountReduce(string(contents))
		//fmt.Println("Result of reduce: ", result)

		//Write results to a single file
		//for k, v := range result {
		//	cwd, _ := os.Getwd()
		//	outPath := fmt.Sprintf("%v/output", cwd)
		//	outfile := fmt.Sprintf("%v/output/out.txt", cwd)
		//	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		//		err = os.Mkdir(outPath, 0777)
		//		check(err)
		//	}
		//	f, err := os.OpenFile(outfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		//	check(err)
		//	f.WriteString(fmt.Sprintf("%v,%v\n", k, v))
		//	f.Close()
		//}
	}
}

func (w *Worker) WordCountMap(contents string) map[string][]string {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(contents, ff)
	m := make(map[string][]string)
	for _, word := range words {
		word := strings.ToLower(word)
		m[word] = append(m[word], fmt.Sprintf("%v", 1))
	}
	return m
}

func (w *Worker) WordCountReduce(contents string) map[string]int {
	result := make(map[string]int)
	lines := strings.Split(contents, "\n")
	for _, l := range lines {
		temp := strings.Split(l, ",")
		if len(temp) > 1 {
			array := strings.Split(strings.ReplaceAll(strings.ReplaceAll(temp[1], "[", ""), "]", ""), " ")
			result[temp[0]] = result[temp[0]] + len(array)
		}
	}
	return result
}

func (w *Worker) SaveToReduceFiles(kvs map[string][]string) {
	pid := os.Getpid()
	for k, v := range kvs {
		reduceKey := ihash(k) % 10
		cwd, _ := os.Getwd()
		basePath := fmt.Sprintf("%v/_temporary/%v/", cwd, pid)

		var path string
		path = fmt.Sprintf("%v/_temporary/%v/reduce_%v", cwd, pid, reduceKey)

		w.ReduceFilePaths[path] = 0 //Append to our filepaths

		if _, err := os.Stat(basePath); os.IsNotExist(err) {
			err = os.Mkdir(basePath, 0777)
			check(err)
		}
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		check(err)
		f.WriteString(fmt.Sprintf("%v,%v\n", k, v))
		f.Close()
	}
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func worker() {
	worker := new(Worker)
	worker.ReduceFilePaths = make(map[string]int)
	for {
		var task Task
		if worker.RunningTask == false {
			worker.requestTask()
			time.Sleep(1 * time.Second)
		} else {
			worker.runTask(task)
			worker.RunningTask = false
		}
	}
}

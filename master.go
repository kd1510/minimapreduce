package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

/*
* we will have an in memory array of MapTaskPartitions for the data to be assigned
* to workers. This will be a shared data structure and can be accessed
* concurrently by workers to ask for work. We need to write tests
* where many workers ask for work concurrently (each can be a thread) and
* check that every task gets processed exactly once.
*
* Also run this on benchmarks here (wikipedia word count)
* Dataset1 50GB https://engineering.purdue.edu/~puma/datasets.htm
*
 */

type JobCoordinator struct {
	runningJob           bool
	mapPhase             bool
	reducePhase          bool
	dataPath             string
	MapTaskPartitions    []string
	ReduceTaskPartitions map[string]int
	ReduceTasks          []int
	workerLocalDataPath  string
}

var mu sync.Mutex

// MapTaskPartitions may be concurrently read/edited by multiple threads
// write a test where many threads call this method concurrently
func (j *JobCoordinator) assignMapTask() Task {
	mu.Lock()
	defer mu.Unlock()
	assigned := j.MapTaskPartitions[0]
	j.MapTaskPartitions = j.MapTaskPartitions[1:]
	taskType := "Map"
	return Task{taskType, assigned, []string{}, 0}
}

func (j *JobCoordinator) assignReduceTask() Task {
	mu.Lock()
	defer mu.Unlock()

	var reduceFiles []string
	for f, _ := range j.ReduceTaskPartitions {
		reduceFiles = append(reduceFiles, f)
	}
	assigned := j.ReduceTasks[0]
	j.ReduceTasks = j.ReduceTasks[1:]
	return Task{"Reduce", "none", reduceFiles, assigned}
}

func (j *JobCoordinator) findPartitions() {
	filepath.Walk(j.dataPath, func(path string, info os.FileInfo, err error) error {
		if err == nil {
			if strings.Contains(info.Name(), ".txt") {
				fullPath, _ := filepath.Abs(path)
				j.MapTaskPartitions = append(j.MapTaskPartitions, fullPath)
			}
		}
		return nil
	})
}

func master() {
	go server()
	for {
		time.Sleep(2 * time.Second)
	}
}

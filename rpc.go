package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Job struct {
	DataLocation string
}

type Task struct {
	TaskType                    string
	PathToPartition             string
	AllReducePartitionLocations []string
	ReduceTaskNumber            int
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Server struct {
	jobCoordinator JobCoordinator
}

func (server *Server) RequestTask(args *int, reply *Task) error {
	if server.jobCoordinator.runningJob == false {
		return nil
	}

	if server.jobCoordinator.runningJob == true {
		//MapPhase
		if server.jobCoordinator.mapPhase == true {
			if len(server.jobCoordinator.MapTaskPartitions) > 0 {
				assignedTask := server.jobCoordinator.assignMapTask()
				*reply = assignedTask
				return nil
			} else {
				server.jobCoordinator.ReduceTasks = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} //hardcoded for now to be R
				server.jobCoordinator.reducePhase = true
				server.jobCoordinator.mapPhase = false
				return nil
			}
		}

		//ReducePhase
		if server.jobCoordinator.reducePhase == true {
			var reduceFiles []string
			for f, _ := range server.jobCoordinator.ReduceTaskPartitions {
				reduceFiles = append(reduceFiles, f)
			}
			if len(server.jobCoordinator.ReduceTasks) > 0 {
				*reply = server.jobCoordinator.assignReduceTask()
				return nil
			} else {
				server.jobCoordinator.runningJob = false
				return nil
			}
		}
	}
	return errors.New("Could not identify current state!")
}

type ReduceFiles struct {
	FilePaths []string
}

func (server *Server) RegisterReduceFiles(files *ReduceFiles, reply *string) error {
	for _, f := range files.FilePaths {
		server.jobCoordinator.ReduceTaskPartitions[f] = 0
	}
	return nil
}

func (server *Server) RegisterNewJob(job *Job, reply *string) error {
	fmt.Println("Received Job!")
	server.jobCoordinator.dataPath = job.DataLocation
	server.jobCoordinator.ReduceTaskPartitions = make(map[string]int)
	server.jobCoordinator.findPartitions()
	server.jobCoordinator.runningJob = true
	server.jobCoordinator.mapPhase = true
	*reply = "Job Successfully Registered"
	return nil
}

func server() {
	server := new(Server)
	rpc.Register(server)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

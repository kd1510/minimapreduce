package main

import (
	"net/rpc"
)

func submit_job(path_to_data string) {
	job := Job{path_to_data}
	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		println("dialing:", err)
	}
	var reply string
	err = client.Call("Server.RegisterNewJob", job, &reply)
	if err != nil {
		println("there is some error!")
	} else {
		println("Submitted work to master!")
		println(reply)
	}
}

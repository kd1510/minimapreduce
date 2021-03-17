package main

import (
	"os"
)

func main() {
	server := os.Args[1]

	if server == "master" {
		master()
	}
	if server == "worker" {
		worker()
	}
	if server == "client" {
		data_path := os.Args[2]
		submit_job(data_path)
	}
}

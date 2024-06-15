package main

import (
	"fmt"
	"os"

	"github.com/gaukas/benchmarkconn/cmd/utils"
)

func main() {
	args := os.Args[1:]

	if len(args) < 3 {
		utils.NewBenchmark().Usage()
		os.Exit(1)
	}

	b := utils.NewBenchmark()

	benchType := os.Args[1]
	benchOp := os.Args[2]
	serverAddr := os.Args[3]

	b.SetBenchType(benchType)
	b.SetCommand(benchOp)
	b.SetAddress(serverAddr)
	if err := b.Init(os.Args[4:]); err != nil {
		fmt.Printf("Failed to initialize benchmark: %v\n", err)
		os.Exit(1)
	}

	if err := b.Client(); err != nil {
		fmt.Printf("Failed to run benchmark: %v\n", err)
		os.Exit(1)
	}
}

package flowprocess_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	flow "github.com/mqy527/flowprocess"
)

func Test_SquareSum(t *testing.T) {
	var sum int64
	var i int64
	start := time.Now()
	for i = 0; i < 10000000; i++ {
		sum += i*i
	}
	duration := time.Since(start)
	fmt.Printf("sum: %v\nduration:%v\n", sum, duration.Nanoseconds())
	fmt.Println("NumGoroutine: " ,runtime.NumGoroutine())
}
func Test_FlowProcess(t *testing.T) {
	fp := flow.FlowProcess{}
	fp.Add(flow.NewFlowUnit([]flow.Processor{
		GenNumProcessor{
			startInclude: 0,
			endExclude:   5000000,
		},
		GenNumProcessor{
			startInclude: 5000000,
			endExclude:   10000000,
		},
	}, 1000))
	fp.Add(flow.NewFlowUnit([]flow.Processor{
		SquareProcessor{},
		SquareProcessor{},
	}, 1000))

	fp.Add(flow.NewFlowUnit([]flow.Processor{
		SumProcessor{},
	}, 1000))

	start := time.Now()
	fp.Start()
	fp.Await()
	duration := time.Since(start)
	fmt.Printf("duration:%v\n", duration.Nanoseconds())
	fmt.Println("NumGoroutine: " ,runtime.NumGoroutine())
}

type GenNumProcessor struct {
	startInclude int64
	endExclude int64
}

func(g GenNumProcessor) Proccess(inTasks flow.TaskChan, outTask flow.TaskChan) {
	for i := g.startInclude; i < g.endExclude; i++ {
		outTask <- i
	}
}

type SquareProcessor struct {
}

func(s SquareProcessor) Proccess(inTasks flow.TaskChan, outTask flow.TaskChan) {
	for task := range inTasks {
		num := task.(int64)
		outTask <- num * num
	}
}

type SumProcessor struct {
}

func(s SumProcessor) Proccess(inTasks flow.TaskChan, outTask flow.TaskChan) {
	var sum int64 = 0
	for task := range inTasks {
		num := task.(int64)
		sum += num
	}
	fmt.Printf("sum: %v\n", sum)
}
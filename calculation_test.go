package flowprocess_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	flow "github.com/mqy527/flowprocess"
)

func Test_calculationGeneralWay(t *testing.T) {
	start := time.Now()
	var sum float64
	var i float64
	var len float64 = 1000000000
	for i = 1; i <= len; i++ {
		sum += math.Sqrt(math.Sqrt(math.Log10(i)))
	}
	duration := time.Since(start)
	fmt.Printf("sum: %6f\nduration(ms):%v\n", sum, duration.Milliseconds())
}

func Test_CalculationFlowWay(t *testing.T) {
	start := time.Now()
	var len float64 = 1000000000
	calProcessorNum := 4
	segmentLen := len / float64(calProcessorNum)

	calProcessors := func() []flow.Processor {
		calProcessors := make([]flow.Processor, calProcessorNum)
		var distributedIndex float64 = 0
		for i := 0; i < calProcessorNum-1; i++ {
			start := 1 + segmentLen*float64(i)
			end := start + segmentLen - 1
			calProcessors[i] = &calculationProcessor{
				start: start,
				end:   end,
			}
			distributedIndex = end
		}
		calProcessors[calProcessorNum-1] = &calculationProcessor{
			start: distributedIndex + 1,
			end:   len,
		}
		return calProcessors

	}()

	sumProcessor := &sumProcessor{0}

	fp := flow.NewFlow(nil, nil, nil)
	//Node-0, 4 calculators
	fp.AddNodeProcessors(calProcessorNum, calProcessors...)
	//Node-1, 1 summarize
	fp.AddNodeProcessors(0, sumProcessor)

	fp.Start()
	fp.Await()
	duration := time.Since(start)
	fmt.Printf("sum: %6f\nduration(ms):%v\n", sumProcessor.sum, duration.Milliseconds())

}

type calculationProcessor struct {
	start float64
	end   float64
}

func (g *calculationProcessor) Proccess(inTasks flow.InTaskChan, outTask flow.OutTaskChan, ctx context.Context) (cancelAllProcess bool) {
	var sum float64
	for i := g.start; i <= g.end; i++ {
		sum += math.Sqrt(math.Sqrt(math.Log10(i)))
	}
	outTask <- sum
	return
}

type sumProcessor struct {
	sum float64
}

func (g *sumProcessor) Proccess(inTasks flow.InTaskChan, outTask flow.OutTaskChan, ctx context.Context) (cancelAllProcess bool) {
	for res := range inTasks {
		g.sum += res.(float64)
	}
	return
}

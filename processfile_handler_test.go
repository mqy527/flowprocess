package flowprocess_test

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/mqy527/flowprocess"
)

func Test_ProcessFileFlowWay_Handler(t *testing.T) {
	start := time.Now()

	//define callback
	taskAt := func(task flowprocess.TraceableTask, nodeId int) {
		log.Println("task:", task.TaskId(), " at node: ", nodeId)
	}
	onNewTaskCreated := func(task flowprocess.TraceableTask, nodeId int) {
		log.Println("task:", task.TaskId(), " created at node: ", nodeId)
	}

	onTaskFinished := func(task flowprocess.TraceableTask, nodeId int, err error) {
		log.Println("task:", task.TaskId(), " finished at node: ", nodeId, ". err: ", err)
	}
	fp := flowprocess.NewFlow(taskAt, onNewTaskCreated, onTaskFinished)

	queneCount := 4000
	//Node-0: read file lines.we define 1 processor to read file.
	fp.AddNodeTaskHandlers(queneCount, &ReadFileTaskHandler{
		Filepath: "testfile/2553.txt",
	})

	//Node-1: split and count.we define 4 parallel processors to split and count.
	fp.AddNodeTaskHandlers(queneCount,
		&SplitAndCountTaskHandler{
			wordCount: map[string]int{},
		},
		&SplitAndCountTaskHandler{
			wordCount: map[string]int{},
		},
		&SplitAndCountTaskHandler{
			wordCount: map[string]int{},
		},
		&SplitAndCountTaskHandler{
			wordCount: map[string]int{},
		},
	)

	//Node-2: summarize the word occurrence.
	fp.AddNodeTaskHandlers(1,
		&SumWordCountTaskHandler{
			wordCount: map[string]int{},
			reverse:   true,
		},
	)

	fp.Start()
	if res, ok := fp.Result(); ok {
		sortedWc := res.(flowprocess.TraceableTask).Inner().([]wordAndCount)
		duration := time.Since(start)
		fmt.Printf("duration(ms):%v\n", duration.Milliseconds())

		topN := 10
		if topN > len(sortedWc) {
			topN = len(sortedWc)
		}
		fmt.Println("sortedWc-top", topN, ":")
		for i := 0; i < topN; i++ {
			fmt.Println(sortedWc[i])
		}
	}
}

// ReadFileProcessor reads file lines, and puts the line into a OutTaskChan for next flow-node to process.
type ReadFileTaskHandler struct {
	flowprocess.TaskHandlerAdapter
	Filepath string
}

func (g ReadFileTaskHandler) Handle(inTask flowprocess.Task, dispatch func(outTask flowprocess.Task) error) (err error) {
	f, err := os.Open(g.Filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	i := 0
	for sc.Scan() {
		i++
		line := sc.Text()
		dispatch(flowprocess.ToTraceableTask(strconv.Itoa(i), line))
	}
	return nil
}

// SplitAndCountProcessor splits the line and counts the word occurrence.
type SplitAndCountTaskHandler struct {
	flowprocess.TaskHandlerAdapter
	wordCount      map[string]int
	processedCount int
}

func (h *SplitAndCountTaskHandler) Handle(inTask flowprocess.Task, dispatch func(outTask flowprocess.Task) error) (err error) {
	line := inTask.(flowprocess.TraceableTask).Inner().(string)
	sps := splitText(line)
	for i := 0; i < len(sps); i++ {
		st := strings.TrimSpace(sps[i])
		if len(st) > 0 {
			h.wordCount[st]++
			h.processedCount++
		}
	}
	return nil
}

func (h *SplitAndCountTaskHandler) OnCompleted(dispatch func(outTask flowprocess.Task) error) (err error) {
	newTask := flowprocess.ToTraceableTask(fmt.Sprintf("%v", (*(*int64)(unsafe.Pointer(h)))), h.wordCount)
	dispatch(newTask)
	return nil
}

//SumWordCountTaskHandler summarizes the word occurrence.
type SumWordCountTaskHandler struct {
	wordCount map[string]int
	reverse   bool
}

func (h *SumWordCountTaskHandler) Handle(inTask flowprocess.Task, dispatch func(outTask flowprocess.Task) error) (err error) {
	wc := inTask.(flowprocess.TraceableTask).Inner().(map[string]int)
	for key, val := range wc {
		h.wordCount[key] += val
	}
	return
}

func (h *SumWordCountTaskHandler) OnCompleted(dispatch func(outTask flowprocess.Task) error) (err error) {
	sortedWc := sortWc(h.wordCount, h.reverse)
	newTask := flowprocess.ToTraceableTask(fmt.Sprintf("%v", (*(*int64)(unsafe.Pointer(h)))), sortedWc)
	dispatch(newTask)
	return nil
}

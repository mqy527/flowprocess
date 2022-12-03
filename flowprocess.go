package flowprocess

import (
	"context"
	"errors"
	"fmt"
	"log"
)

type Task interface{}

type TaskChan chan Task
type InTaskChan <-chan Task
type OutTaskChan chan<- Task

// Flow 表示流处理引擎。
type Flow interface {
	// AddNodeProcessors 添加流处理节点。
	// processorChanBufferSize 表示该节点的输出通道大小。
	// processors 为处理逻辑，每个Processor占用单独的goroutine，此方法开放性好。
	AddNodeProcessors(processorChanBufferSize int, processors ...Processor)
	// AddNodeTaskHandlers 添加流处理节点。
	// processorChanBufferSize 表示该节点的输出通道大小。
	// taskHandlers 为处理逻辑，每个TaskHandler占用单独的goroutine，当需要追踪任务执行轨迹时，可以使用该方法。
	AddNodeTaskHandlers(processorChanBufferSize int, taskHandlers ...TaskHandler)
	// Start 启动流处理引擎。
	Start()
	// Await 等待流处理引擎执行完成。
	Await()
	// Result 等待执行结果。该方法会阻塞，直到有结果输出。当ok为true时，结果有效。
	// 当任务的执行结果有多个时，可循环调用该方法获取结果。
	Result() (result interface{}, ok bool)
	// SubmitTaskAfterStart 在flow启动后, 向某个node（nodeId从1开始，node0用于产生任务，所以nodeId从1开始），手动提交任务。
	SubmitTaskAfterStart(nodeId int, task Task) error
}

// flow 表示流处理引擎。流处理逻辑为：node0(process) --result0--> node1(process) --result1--> node2(process)等等.
type flow struct {
	// nodes 代表流处理节点，每个节点的输出为下一个节点的输入。
	nodes      []Node
	resultChan <-chan Task
	// inTaskChans 是各个Node的入参chan，在启动flow后，可用于手动再次提交任务。
	inTaskChans []chan<- Task
	// taskAt 用于跟踪任务。当TraceableTask流转到某个节点时，回调该方法。
	taskAt           func(task TraceableTask, nodeId int)
	// onNewTaskCreated 当新的TraceableTask被创建时，回调该方法。
	onNewTaskCreated func(task TraceableTask, nodeId int)
	// onTaskFinished 用于跟踪任务。当TraceableTask结束时，回调该方法。
	onTaskFinished   func(task TraceableTask, nodeId int, err error)
}

// NewFlow 创建流处理引擎。
// taskAt： 当TraceableTask到达某节点时，回调该方法。
// onNewTaskCreated： 当新的TraceableTask产生时，回调该方法。
// onTaskFinished： 当onTaskFinished结束时，回调该方法。
// 只有通过Flow.AddNodeTaskHandlers创建流节点，且task为TraceableTask时，才会回调上述方法。
func NewFlow(taskAt func(task TraceableTask, nodeId int),
	onNewTaskCreated func(task TraceableTask, nodeId int),
	onTaskFinished func(task TraceableTask, nodeId int, err error)) Flow {
	f := &flow{
		nodes:            []Node{},
		resultChan:       make(<-chan Task),
		taskAt:           taskAt,
		onNewTaskCreated: onNewTaskCreated,
		onTaskFinished:   onTaskFinished,
	}
	if f.taskAt == nil {
		f.taskAt = func(task TraceableTask, nodeId int) {
		}
	}
	if f.onNewTaskCreated == nil {
		f.onNewTaskCreated = func(task TraceableTask, nodeId int) {
		}
	}
	if f.onTaskFinished == nil {
		f.onTaskFinished = func(task TraceableTask, nodeId int, err error) {
		}
	}

	return f
}
// addNode 添加一个节点。
func (f *flow) addNode(node Node) {
	f.nodes = append(f.nodes, node)
}

func (f *flow) AddNodeProcessors(processorChanBufferSize int, processors ...Processor) {
	var preNode Node = nil
	if len(f.nodes) > 0 {
		preNode = f.nodes[len(f.nodes)-1]
	}
	f.addNode(NewNode(processors, processorChanBufferSize, len(f.nodes), preNode))
}

func (f *flow) AddNodeTaskHandlers(processorChanBufferSize int, taskHandlers ...TaskHandler) {
	handler := NewTaskHandlerProcessors(f.taskAt, f.onNewTaskCreated, f.onTaskFinished, taskHandlers...)
	var preNode Node = nil
	if len(f.nodes) > 0 {
		preNode = f.nodes[len(f.nodes)-1]
	}
	f.addNode(NewNode(handler, processorChanBufferSize, len(f.nodes), preNode))
}

// Start starts the flow process
func (f *flow) Start() {
	f.inTaskChans = make([]chan<- Task, len(f.nodes))
	var taskChan TaskChan = nil
	for i, node := range f.nodes {
		f.inTaskChans[i] = taskChan
		taskChan = node.Process(taskChan)
	}
	f.resultChan = taskChan
}

func (f *flow) Await() {
	for _, pu := range f.nodes {
		<-pu.Done()
	}
}

func (f *flow) Result() (result interface{}, ok bool) {
	result, ok = <-f.resultChan
	if ok {
		traceableTask, traceable := result.(TraceableTask)
		if traceable {
			f.onTaskFinished(traceableTask, len(f.nodes)-1, nil)
		}
	}
	return
}

func (f *flow) SubmitTaskAfterStart(nodeId int, task Task) error {
	if nodeId <= 0 {
		return errors.New("nodeId must >= 1")
	}
	if len(f.inTaskChans) == 0 {
		return errors.New("please check whether the flow is started")
	}
	if nodeId >= len(f.inTaskChans) {
		return fmt.Errorf("nodeId[%d] overflow, max nodeId is %d", nodeId, len(f.inTaskChans)-1)
	}
	f.inTaskChans[nodeId] <- task
	return nil
}

// Node defines a node to process tasks
type Node interface {
	// Process processes tasks from in-task chan, and put the result into the out-task chan.
	Process(in TaskChan) (out TaskChan)
	Done() <-chan struct{}
	// Cancel cancels the task execution，the submitted tasks will be executed.
	Cancel()
}

// Processor processes task from the previous FlowNode, and place the result into outTask chan which will be proccessed by the next FlowNode.
// A node includes multiple processors which run concurrently.
type Processor interface {
	Proccess(inTasks InTaskChan, outTask OutTaskChan, ctx context.Context) (cancelAllProcess bool)
}

type DefaultProcessor struct {
	handleAndDispatch TaskHandler
	taskAt            func(task TraceableTask, nodeId int)
	onNewTaskCreated  func(task TraceableTask, nodeId int)
	onTaskFinished    func(task TraceableTask, nodeId int, err error)
}

func (p *DefaultProcessor) Proccess(inTaskChan InTaskChan, outTaskChan OutTaskChan, ctx context.Context) (cancelAllProcess bool) {
	nodeId := ctx.Value(NODE_ID).(int)

	if inTaskChan == nil {
		err := p.handleAndDispatch.Handle(nil, func(outTask Task) error {
			traceableOutTask, outTaskTraceable := outTask.(TraceableTask)
			p.tryTraceCreateTask(outTaskTraceable, traceableOutTask, nodeId)
			//to prevent blocking when task is cancelled
			select {
			case <-ctx.Done():
				return errors.New("task execution is cancelled, dispatch fail")
			case outTaskChan <- outTask:
				return nil
			}
		})
		if err != nil {
			log.Printf("HandleAndDispatch error when inTaskChan is nil: %v. Begin canceling all processes.", err)
			return true
		}
		return false
	}

	for {
		select {
		case <-ctx.Done():
			return true
		case task, ok := <-inTaskChan:
			var traceableTask TraceableTask
			var traceable bool
			if ok {
				traceableTask, traceable = task.(TraceableTask)
				if traceable {
					p.taskAt(traceableTask, nodeId)
				}
			}
			thisTaskDispatched := false
			dispatchFunc := func(outTask Task) error {
				thisTaskDispatched = true
				traceableOutTask, outTaskTraceable := outTask.(TraceableTask)

				if outTaskTraceable && traceable {
					if traceableTask.TaskId() != traceableOutTask.TaskId() {
						p.onTaskFinished(traceableTask, nodeId, nil)
						p.onNewTaskCreated(traceableOutTask, nodeId)
					}
				} else {
					p.tryTraceFinishedTask(traceable, traceableTask, nodeId, nil)
					p.tryTraceCreateTask(outTaskTraceable, traceableOutTask, nodeId)
				}
				//to prevent blocking when task is cancelled
				select {
				case <-ctx.Done():
					return errors.New("task execution is cancelled, dispatch fail")
				case outTaskChan <- outTask:
					return nil
				}
			}
			var err error
			if ok {
				err = p.handleAndDispatch.Handle(task, dispatchFunc)
			} else {
				err = p.handleAndDispatch.OnCompleted(dispatchFunc)
			}
			if err != nil {
				p.tryTraceFinishedTask(traceable, traceableTask, nodeId, err)
				log.Printf("HandleAndDispatch error: %v. Begin canceling all processes.", err)
				return true
			}
			if !thisTaskDispatched {
				p.tryTraceFinishedTask(traceable, traceableTask, nodeId, nil)
			}
			if !ok {
				return false
			}
		}
	}
}

func (p *DefaultProcessor) tryTraceFinishedTask(traceable bool, traceableTask TraceableTask, nodeId int, err error) {
	if traceable {
		p.onTaskFinished(traceableTask, nodeId, err)
	}
}

func (p *DefaultProcessor) tryTraceCreateTask(traceable bool, traceableTask TraceableTask, nodeId int) {
	if traceable {
		p.onNewTaskCreated(traceableTask, nodeId)
	}
}

func NewTaskHandlerProcessors(
	taskAt func(task TraceableTask, nodeId int),
	onNewTaskCreated func(task TraceableTask, nodeId int),
	onTaskFinished func(task TraceableTask, nodeId int, err error),
	taskhandlers ...TaskHandler) []Processor {
	ps := make([]Processor, len(taskhandlers))

	for i := 0; i < len(taskhandlers); i++ {
		ps[i] = &DefaultProcessor{
			handleAndDispatch: taskhandlers[i],
			taskAt:            taskAt,
			onNewTaskCreated:  onNewTaskCreated,
			onTaskFinished:    onTaskFinished,
		}
	}
	return ps
}

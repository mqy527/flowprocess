package flowprocess

// TraceableTask 表示可追踪的task。
type TraceableTask interface {
	// TaskId 用于获取TaskId，便于追踪。
	TaskId() string
	// Inner 用于获取底层的Task。
	Inner() Task
}

type traceableTask struct {
	taskId string
	task   Task
}

func (t traceableTask) TaskId() string {
	return t.taskId
}
func (t traceableTask) Inner() Task {
	return t.task
}

// ToTraceableTask 将Task转为TraceableTask。
func ToTraceableTask(taskId string, task Task) TraceableTask {
	t := traceableTask{taskId: taskId, task: task}
	return t
}

// TaskHandler 定义任务处理逻辑。
type TaskHandler interface {
	// Handle 处理任务。如果返回的err不为nil，则会中断流处理。
	// dispatch 用于转发任务至下一个节点。
	Handle(inTask Task, dispatch func(outTask Task) error) (err error)

	// OnCompleted 当前节点的任务处理完毕时，回调该方法。如果返回的err不为nil，则会中断流处理。
	OnCompleted(dispatch func(outTask Task) error) (err error)
}

type TaskHandlerAdapter struct {
}

func (h TaskHandlerAdapter) OnCompleted(dispatch func(outTask Task) error) (err error) {
	return nil
}

package flowprocess

import (
	"context"
	"sync"
)

type NodeKey string

const (
	NODE_ID NodeKey = "nodeId"
)
// node 定义了流处理引擎节点
type node struct {
	processors              []Processor
	processorChanBufferSize int
	done                    chan struct{}
	id                      int
	ctx                     context.Context
	cancelFunc              context.CancelFunc
	preNode                 Node
}
// NewNode 创建流处理引擎节点。processors 表示处理逻辑，每个 Processor独占一个goroutine。processorChanBufferSize 为输出通道的大小。id 为节点标识。
// preNode 指向上一个节点，取消任务时，会调用preNode.Cancel()。
func NewNode(processors []Processor, processorChanBufferSize int, id int, preNode Node) Node {
	ctx, cancelFunc := context.WithCancel(context.WithValue(context.Background(), NODE_ID, id))
	pu := &node{
		processors:              processors,
		processorChanBufferSize: processorChanBufferSize,
		id:                      id,
		ctx:                     ctx,
		cancelFunc:              cancelFunc,
		preNode:                 preNode,
	}
	return pu
}

func (pu *node) Process(in TaskChan) (out TaskChan) {
	pu.done = make(chan struct{})

	outchain := make(TaskChan, pu.processorChanBufferSize)

	var wg sync.WaitGroup
	wg.Add(len(pu.processors))

	for i, processor := range pu.processors {
		go func(index int, processor Processor, inTaskChan <-chan Task, outTaskChan chan<- Task) {
			cancelAllProcess := processor.Proccess(inTaskChan, outTaskChan, pu.ctx)
			if cancelAllProcess {
				pu.Cancel()
			}
			wg.Done()
		}(i, processor, in, outchain)
	}

	go func() {
		wg.Wait()
		pu.Cancel()
		close(outchain)
		close(pu.done)
	}()

	return outchain
}

func (pu *node) Cancel() {
	if pu.preNode != nil {
		pu.preNode.Cancel()
	}
	pu.cancelFunc()
}

func (pu *node) Done() <-chan struct{} {
	return pu.done
}

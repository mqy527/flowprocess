package flowprocess

import (
	"sync"
)

type Task interface{}

type TaskChan chan Task

// eg: FlowUnit1(process) --result1--> FlowUnit2(process) --result2--> FlowUnit3(process)
type FlowProcess struct {
	flowUnits []FlowUnit
}

func (p *FlowProcess) Add(pu FlowUnit) {
	p.flowUnits = append(p.flowUnits, pu)
}

//start flow process
func (p *FlowProcess) Start() {
	var taskChans []TaskChan
	for _, pu := range p.flowUnits {
		taskChans = pu.UnitProcess(taskChans)
	}
}

func (p *FlowProcess) Await() {
	for _, pu := range p.flowUnits {
		<-pu.Done()
	}
}

//a node to process tasks
type FlowUnit interface {
	UnitProcess([]TaskChan) []TaskChan
	Done() <-chan struct{}
}

//process task from last FlowUnit, and place the result into outTask chan, which will be proccessed by next FlowUnit
type Processor interface {
	Proccess(inTasks TaskChan, outTask TaskChan)
}

type flowUnit struct {
	processors     []Processor
	processorChanBufferSize int
	done           chan struct{}
}

func NewFlowUnit(processors []Processor, processorChanBufferSize int) FlowUnit {
	pu := &flowUnit{
		processors:     processors,
		processorChanBufferSize: processorChanBufferSize,
	}
	return pu
}

func (pu *flowUnit) UnitProcess(inTaskChains []TaskChan) []TaskChan {
	pu.done = make(chan struct{})

	outchains := make([]TaskChan, len(pu.processors))
	for i := 0; i < len(outchains); i++ {
		outchains[i] = make(TaskChan, pu.processorChanBufferSize)
	}

	var wg sync.WaitGroup
	wg.Add(len(pu.processors))
	var ts TaskChan = nil
	if inTaskChains != nil {
		ts = merge(inTaskChains, len(pu.processors)*pu.processorChanBufferSize)
	}
	
	for i, processor := range pu.processors {
		go func (index int, processor Processor, outTaskChan TaskChan)  {
			processor.Proccess(ts, outTaskChan)
			wg.Done()
			close(outTaskChan)
		}(i, processor, outchains[i])
	}

	go func() {
		wg.Wait()
		close(pu.done)
	}()

	return outchains
}

func (pu *flowUnit) Done() <-chan struct{} {
	return pu.done
}


func merge(TaskChans []TaskChan, chainSize int) TaskChan {
	ts := make(TaskChan, chainSize)
	var wg sync.WaitGroup
	wg.Add(len(TaskChans))
	for _, taskChan := range TaskChans {
		go func(taskChan TaskChan) {
			for task := range taskChan {
				ts <- task
			}
			wg.Done()
		}(taskChan)
	}
	go func() {
		wg.Wait()
		close(ts)
	}()
	return ts
}

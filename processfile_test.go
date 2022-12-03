package flowprocess_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mqy527/flowprocess"
)

func Test_ProcessFileGeneralWay(t *testing.T) {
	wordCount := map[string]int{}
	reverse := true
	//You can replace the file with a larger file.
	file := "testfile/2553.txt"
	start := time.Now()
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	//split lines
	for sc.Scan() {
		line := sc.Text()
		sps := splitText(line)
		for i := 0; i < len(sps); i++ {
			st := strings.TrimSpace(sps[i])
			if len(st) > 0 {
				wordCount[st]++
			}
		}
	}
	//sort by word occurrence times desc
	sortedWc := sortWc(wordCount, reverse)

	duration := time.Since(start)

	//print elapsed time
	fmt.Printf("duration(ms):%v\n", duration.Milliseconds())

	//print topN
	topN := 10
	if topN > len(sortedWc) {
		topN = len(sortedWc)
	}
	fmt.Println("sortedWc-top", topN, ":")
	for i := 0; i < topN; i++ {
		fmt.Println(sortedWc[i])
	}
}

func Test_ProcessFileFlowWay(t *testing.T) {
	start := time.Now()
	fp := flowprocess.NewFlow(nil, nil, nil)
	queneCount := 4000
	//Node-0: read file lines. We define 1 processor to read file.
	fp.AddNodeProcessors(queneCount,
		&ReadFileProcessor{
			//You can replace the file with a larger file.
			Filepath: "testfile/2553.txt",
		})

	//Node-1: split and count. we define 4 parallel processors to split and count.
	fp.AddNodeProcessors(queneCount,
		&SplitAndCountProcessor{},
		&SplitAndCountProcessor{},
		&SplitAndCountProcessor{},
		&SplitAndCountProcessor{},
	)

	result := &SumWordCountProcessor{
		reverse: true,
	}

	//Node-2: we define 1 processor to summarize.
	fp.AddNodeProcessors(1,
		result,
	)

	fp.Start()
	if res, ok := fp.Result(); ok {
		sortedWc := res.([]wordAndCount)
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

//ReadFileProcessor reads file lines, and put the line into a OutTaskChan for next flow-node to process.
type ReadFileProcessor struct {
	Filepath string
}

func (g *ReadFileProcessor) Proccess(inTasks flowprocess.InTaskChan, outTask flowprocess.OutTaskChan, ctx context.Context) (cancelAllProcess bool) {
	f, err := os.Open(g.Filepath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			line := sc.Text()
			outTask <- line
		}
	}
	return
}

//SplitAndCountProcessor splits the line and counts the word occurrence.
type SplitAndCountProcessor struct {
}

func (s *SplitAndCountProcessor) Proccess(inTasks flowprocess.InTaskChan, outTask flowprocess.OutTaskChan, ctx context.Context) (cancelAllProcess bool) {
	wordCount := map[string]int{}
	for {
		select {
		case <-ctx.Done():
			return true
		case task, ok := <-inTasks:
			if ok {
				line := task.(string)
				sps := splitText(line)
				for i := 0; i < len(sps); i++ {
					st := strings.TrimSpace(sps[i])
					if len(st) > 0 {
						wordCount[st]++
					}
				}
			} else {
				outTask <- wordCount
				return
			}
		}
	}
}

func splitText(text string) []string {
	text = strings.ReplaceAll(text, ".", "")
	text = strings.ReplaceAll(text, ",", "")
	text = strings.ReplaceAll(text, "?", "")
	text = strings.ReplaceAll(text, "!", "")
	text = strings.ReplaceAll(text, ":", "")
	text = strings.ReplaceAll(text, "\"", "")
	text = strings.ReplaceAll(text, "(", "")
	text = strings.ReplaceAll(text, ")", "")
	if len(text) > 0 {
		sps := strings.Split(text, " ")
		return sps
	} else {
		return nil
	}
}

//SumWordCountProcessor summarizes the word occurrence.
type SumWordCountProcessor struct {
	reverse bool
}

func (s *SumWordCountProcessor) Proccess(inTasks flowprocess.InTaskChan, outTask flowprocess.OutTaskChan, ctx context.Context) (cancelAllProcess bool) {
	wordCount := map[string]int{}
	for {
		select {
		case <-ctx.Done():
			return true
		case task, ok := <-inTasks:
			if ok {
				wc := task.(map[string]int)
				for key, val := range wc {
					wordCount[key] += val
				}
			} else {
				sortedWc := sortWc(wordCount, s.reverse)
				outTask <- sortedWc
				return
			}
		}
	}
}

func sortWc(wc map[string]int, reverse bool) []wordAndCount {
	wcArr := make(wordAndCounts, len(wc))
	i := 0
	for key, val := range wc {
		wcArr[i] = wordAndCount{
			word:  key,
			count: val,
		}
		i++
	}
	if reverse {
		sort.Sort(sort.Reverse(wcArr))
		return wcArr
	}
	sort.Sort(wcArr)
	return wcArr
}

type wordAndCount struct {
	word  string
	count int
}

type wordAndCounts []wordAndCount

func (p wordAndCounts) Len() int           { return len(p) }
func (p wordAndCounts) Less(i, j int) bool { return p[i].count < p[j].count }
func (p wordAndCounts) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

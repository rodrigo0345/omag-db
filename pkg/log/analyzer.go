package log

import "time"

type AnalyzerMsg struct {
	operation   string
	table       string
	start       time.Time
	took        time.Duration
	description string
}

func (a *Analyzer) BeginMsg(operation string, table string, description string) *AnalyzerMsg {
	msg := &AnalyzerMsg{}
	msg.operation = operation
	msg.table = table
	msg.description = description
	msg.start = time.Now()
	return msg
}

type Analyzer struct {
	blocks []AnalyzerMsg
}

func NewAnalyzer() *Analyzer {
	blocks := make([]AnalyzerMsg, 10)
	return &Analyzer{
		blocks: blocks,
	}
}

func (a *Analyzer) AppendMsg(msg *AnalyzerMsg) {
	msg.took = time.Since(msg.start)
	a.blocks = append(a.blocks, *msg)
}

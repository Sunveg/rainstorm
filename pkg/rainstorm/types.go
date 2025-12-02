package rainstorm

type TupleID struct {
	SourceStage int
	SourceTask  int
	Seq         uint64
}

type Tuple struct {
	JobID      string
	StageID    int
	FromTaskID int
	ID         TupleID
	Key        string
	Value      string
}

type TaskID struct {
	JobID    string
	StageID  int
	Sequence int
}

type Endpoint struct {
	Host string
	Port int
}

type TaskSpec struct {
	ID          TaskID
	OpExe       string
	OpArgs      []string
	IsSource    bool
	IsSink      bool
	ExactlyOnce bool
}

type TaskInfo struct {
	Spec     TaskSpec
	VM       string
	PID      int
	Endpoint Endpoint
	LocalLog string
}

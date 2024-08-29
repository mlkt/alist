package kopia

import "time"

type SnapshotResponse struct {
	Snapshots       []Snapshot `json:"snapshots"`
	UnfilteredCount int        `json:"unfilteredCount"`
	UniqueCount     int        `json:"uniqueCount"`
}

type Snapshot struct {
	ID          string    `json:"id"`
	Description string    `json:"description"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	Summary     Summary   `json:"summary"`
	RootID      string    `json:"rootID"`
	Retention   []string  `json:"retention"`
	Pins        []string  `json:"pins"`
}

type Summary struct {
	Size      int64     `json:"size"`
	Files     int       `json:"files"`
	Symlinks  int       `json:"symlinks"`
	Dirs      int       `json:"dirs"`
	MaxTime   time.Time `json:"maxTime"`
	NumFailed int       `json:"numFailed"`
}

type FileResponse struct {
	Stream  string  `json:"stream"`
	Entries []Entry `json:"entries"`
	Summary Summary `json:"summary"`
}

type Entry struct {
	Name    string    `json:"name"`
	Type    string    `json:"type"`
	Mode    string    `json:"mode"`
	Size    int64     `json:"size"`
	MTime   time.Time `json:"mtime"`
	Obj     string    `json:"obj"`
	Summary Summary   `json:"summ"`
}

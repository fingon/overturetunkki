package worker

import "time"

const (
	tileStagePlanning    = "planning"
	tileStageMaterialize = "materialize"
	tileStageCopy        = "copy"
	tileStageValidation  = "validation"
	tileStageComplete    = "complete"
)

type TileDiagnostics struct {
	ProfileEnabled     bool
	MaterializeProfile string
	Stage              string
	SelectedAssetCount int
	SelectedAssetBytes int64
	PlanningMS         int64
	MaterializeMS      int64
	CopyMS             int64
	ValidationMS       int64
}

func (diagnostics *TileDiagnostics) start(stage string) func() {
	if diagnostics == nil {
		return func() {}
	}
	diagnostics.Stage = stage
	started := time.Now()
	return func() {
		elapsedMS := time.Since(started).Milliseconds()
		switch stage {
		case tileStagePlanning:
			diagnostics.PlanningMS = elapsedMS
		case tileStageMaterialize:
			diagnostics.MaterializeMS = elapsedMS
		case tileStageCopy:
			diagnostics.CopyMS = elapsedMS
		case tileStageValidation:
			diagnostics.ValidationMS = elapsedMS
		}
	}
}

package transfer

import (
	"github.com/chengshiwen/influx-proxy/backend"
	"sync"
)

type Stats struct {
	DatabaseTotal    int32 `json:"database_total"`
	DatabaseDone     int32 `json:"database_done"`
	MeasurementTotal int32 `json:"measurement_total"`
	MeasurementDone  int32 `json:"measurement_done"`
	TransferCount    int32 `json:"transfer_count"`
	InPlaceCount     int32 `json:"inplace_count"`
}

type CircleState struct {
	*backend.Circle
	Stats        map[string]*Stats
	Transferring bool
	wg           sync.WaitGroup
}

func NewCircleState(cfg *backend.CircleConfig, circle *backend.Circle) (cs *CircleState) {
	cs = &CircleState{
		Circle:       circle,
		Stats:        make(map[string]*Stats),
		Transferring: false,
	}
	for _, bkcfg := range cfg.Backends {
		cs.Stats[bkcfg.Url] = &Stats{}
	}
	return
}

func (cs *CircleState) ResetStates() {
	for _, s := range cs.Stats {
		s.DatabaseTotal = 0
		s.DatabaseDone = 0
		s.MeasurementTotal = 0
		s.MeasurementDone = 0
		s.TransferCount = 0
		s.InPlaceCount = 0
	}
}

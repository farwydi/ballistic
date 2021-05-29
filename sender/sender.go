package sender

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/farwydi/ballistic"
	"github.com/farwydi/ballistic/queue/file"
	"github.com/farwydi/ballistic/queue/memory"
	"sync/atomic"
	"time"
)

func NewSender(connect *sql.DB, config ...Config) *Sender {
	// Set default config
	cfg := configDefault(config...)

	logger, _ := NewStdLogger()
	if cfg.Logger != nil {
		logger = cfg.Logger
	}

	return &Sender{
		cfg: cfg,
		filePool: NewPool(
			func(model ballistic.DataModel) (ballistic.Queue, error) {
				return file.NewQueueByModel(model, file.Config{
					Workspace:  cfg.FileWorkspace,
					MaxHistory: 0,
				})
			},
		),
		memoryPool: NewPool(func(_ ballistic.DataModel) (ballistic.Queue, error) {
			return memory.NewQueue(), nil
		}),
		stopSig: make(chan bool),
		connect: connect,
		logger:  logger,
	}
}

type Sender struct {
	cfg Config

	logger Logger

	filePool   ballistic.Pool
	memoryPool ballistic.Pool

	stopSig  chan bool
	connect  *sql.DB
	shutdown int32
}

func (s *Sender) Stop(sendTail bool) {
	atomic.StoreInt32(&s.shutdown, 1)
	s.stopSig <- sendTail
	<-s.stopSig
}

func (s *Sender) Push(model ballistic.DataModel) error {
	if atomic.LoadInt32(&s.shutdown) == 0 {
		err := s.filePool.Push(model)
		if err != nil {
			if s.cfg.UseMemoryFallback {
				s.logger.Warnw("writing to disk failed", "error", err)

				// the memory queue does not return an error
				_ = s.memoryPool.Push(model)
				return nil
			}
			return fmt.Errorf("writing to disk failed: %v", err)
		}
		return nil
	}

	return errors.New("sender shutdown")
}

func (s *Sender) publish(query string, dataModels []ballistic.DataModel) error {
	panicked := true
	tx, err := s.connect.Begin()
	if err != nil {
		return err
	}
	defer func() {
		// Make sure to rollback when panic, Block error or Commit error
		if panicked || err != nil {
			if err := tx.Rollback(); err != nil {
				s.logger.Errorw("problem when rolling back a transaction", "error", err)
			}
		}
	}()

	err = func() error {
		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}

		for _, dataModel := range dataModels {
			args := dataModel.ToExec()
			_, err := stmt.Exec(args...)
			if err != nil {
				return err
			}
		}

		err = stmt.Close()
		if err != nil {
			return err
		}

		return nil
	}()

	if err == nil {
		err = tx.Commit()
	}

	panicked = false

	return err
}

func (s *Sender) fallback(dataModels []ballistic.DataModel, memorySafe bool) {
	if err := s.filePool.Append(dataModels); err != nil {
		if memorySafe {
			_ = s.memoryPool.Append(dataModels)
			s.logger.Warnw("error when fallback a write to disk", "error", err)
			return
		}

		s.logger.Errorw("data lost! fatal error when fallback a write to disk",
			"error", err,
			"lost", len(dataModels),
		)
	}
}

func (s *Sender) RunPusher(period time.Duration, limit int) {
	if period < time.Millisecond {
		period = time.Millisecond
	}

	t := time.NewTicker(period)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				extractSize := 0
				safes := map[string][]ballistic.DataModel{}
				ejectModels, _ := s.memoryPool.Eject(limit)
				extractSize += len(ejectModels)
				for _, dataModel := range ejectModels {
					query := dataModel.SQL()
					safes[query] = append(safes[query], dataModel)
				}

				extractCount := limit - extractSize
				if extractCount > 0 {
					ejectModels, err := s.filePool.Eject(extractCount)
					extractSize += len(ejectModels)
					if err != nil {
						s.logger.Warnw("problem ejecting queue from disk", "error", err)
					}
					for _, dataModel := range ejectModels {
						query := dataModel.SQL()
						safes[query] = append(safes[query], dataModel)
					}
				}

				for query, dataModels := range safes {
					err := s.publish(query, dataModels)
					if err != nil {
						s.logger.Warnw("publication ended with an error", "error", err)
						s.fallback(dataModels, s.cfg.UseMemoryFallback)
					} else {
						if s.cfg.ShowSuccessfulInfo {
							s.logger.Infow("successfully sent", "count", len(dataModels))
						}
					}
				}
			case sendTail := <-s.stopSig:
				ejectModels, _ := s.memoryPool.Eject(-1)
				if !sendTail {
					if len(ejectModels) > 0 {
						if err := s.filePool.Append(ejectModels); err != nil {
							s.logger.Errorw("data lost! fatal error writing to disk when stopping sender",
								"error", err,
								"lost", len(ejectModels),
							)
						}
					}
					close(s.stopSig)
					return
				}

				safes := map[string][]ballistic.DataModel{}

				// From memory
				for _, dataModel := range ejectModels {
					query := dataModel.SQL()
					safes[query] = append(safes[query], dataModel)
				}

				// From file
				ejectModels, err := s.filePool.Eject(-1)
				if err != nil {
					s.logger.Warnw("problem ejecting queue from disk", "error", err)
				}
				for _, dataModel := range ejectModels {
					query := dataModel.SQL()
					safes[query] = append(safes[query], dataModel)
				}

				for query, dataModels := range safes {
					err := s.publish(query, dataModels)
					if err != nil {
						s.logger.Warnw("publication ended with an error", "error", err)
						s.fallback(dataModels, false)
					}
				}

				close(s.stopSig)
				return
			}
		}
	}()
}

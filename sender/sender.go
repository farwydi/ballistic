package sender

import (
	"context"
	"database/sql"
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

	logger := cfg.Logger
	if cfg.Logger == nil {
		logger, _ = NewStdLogger()
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

	isShutdown int32
	stopSig    chan bool
	connect    *sql.DB
}

func (s *Sender) Stop(sendTail bool) {
	if atomic.LoadInt32(&s.isShutdown) == 1 {
		s.logger.Warnw("sender is shutdown")
		return
	}

	s.stopSig <- sendTail
	<-s.stopSig
}

func (s *Sender) Push(model ballistic.DataModel) error {
	if atomic.LoadInt32(&s.isShutdown) == 1 {
		return fmt.Errorf("sender is shutdown")
	}

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

func (s *Sender) publish(ctx context.Context, query string, dataModels []ballistic.DataModel) error {
	panicked := true
	tx, err := s.connect.BeginTx(ctx, nil)
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
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return err
		}

		for _, dataModel := range dataModels {
			args := dataModel.ToExec()
			_, err := stmt.ExecContext(ctx, args...)
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

func (s *Sender) send(ctx context.Context) {
	extractSize := 0
	safes := map[string][]ballistic.DataModel{}
	ejectModels, _ := s.memoryPool.Eject(s.cfg.SendLimit)
	extractSize += len(ejectModels)
	for _, dataModel := range ejectModels {
		query := dataModel.SQL()
		safes[query] = append(safes[query], dataModel)
	}

	extractCount := s.cfg.SendLimit - extractSize
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
		err := s.publish(ctx, query, dataModels)
		if err != nil {
			s.logger.Warnw("publication ended with an error", "error", err)
			s.fallback(dataModels, s.cfg.UseMemoryFallback)
		} else {
			if s.cfg.ShowSuccessfulInfo {
				s.logger.Infow("successfully sent", "count", len(dataModels))
			}
		}
	}
}

func (s *Sender) stop(ctx context.Context, sendTail bool) {
	atomic.StoreInt32(&s.isShutdown, 1)

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
		err := s.publish(ctx, query, dataModels)
		if err != nil {
			s.logger.Warnw("publication ended with an error", "error", err)
			s.fallback(dataModels, false)
		}
	}

	close(s.stopSig)
}

func (s *Sender) RunPusher(ctx context.Context) {
	t := time.NewTicker(s.cfg.SendInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.send(ctx)
		case sendTail := <-s.stopSig:
			s.stop(ctx, sendTail)
			return
		case <-ctx.Done():
			s.stop(context.Background(), false)
			return
		}
	}
}

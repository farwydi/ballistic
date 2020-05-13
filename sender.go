package ballistic

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"time"
)

func NewSender(dumper Dumper, connect *sql.DB) *Sender {
	return &Sender{
		dumper:  dumper,
		route:   map[string][][]interface{}{},
		stopSig: make(chan struct{}),
		connect: connect,
	}
}

type Sender struct {
	dumper   Dumper
	mx       sync.Mutex
	route    map[string][][]interface{}
	stopSig  chan struct{}
	connect  *sql.DB
	activity int32
}

func (s *Sender) Stop() {
	atomic.StoreInt32(&s.activity, int32(1))
	s.stopSig <- struct{}{}
}

func (s *Sender) Push(query string, args ...interface{}) {
	if atomic.LoadInt32(&s.activity) != 0 {
		s.dumper.Dump(query, [][]interface{}{args})
		return
	}
	s.mx.Lock()
	defer s.mx.Unlock()
	s.route[query] = append(s.route[query], args)
}

func (s *Sender) publish(query string, rows [][]interface{}) error {
	tx, err := s.connect.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	for _, args := range rows {
		_, err := stmt.Exec(args)
		if err != nil {
			return err
		}
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *Sender) RunPusher(period time.Duration) {
	if period < time.Millisecond {
		period = time.Second
	}

	t := time.NewTicker(period)
	sender := s
	go func() {
		for {
			select {
			case <-t.C:
				safes := map[string][][]interface{}{}
				for exist, query, row := sender.dumper.Return(); exist; {
					safes[query] = append(safes[query], row)
				}

				for query, rows := range safes {
					err := sender.publish(query, rows)
					if err != nil {
						sender.dumper.Dump(query, rows)
					}
				}

				// Make copy
				sender.mx.Lock()
				safes = map[string][][]interface{}{}
				for query, rows := range sender.route {
					safes[query] = rows
				}
				sender.route = map[string][][]interface{}{}
				sender.mx.Unlock()

				for query, rows := range safes {
					err := sender.publish(query, rows)
					if err != nil {
						sender.dumper.Dump(query, rows)
						return
					}
					delete(safes, query)
				}
			case <-sender.stopSig:
				sender.mx.Lock()
				for query, rows := range sender.route {
					sender.dumper.Dump(query, rows)
				}
				sender.mx.Unlock()
				return
			}
		}
	}()
}

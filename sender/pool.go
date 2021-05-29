package sender

import (
	"github.com/farwydi/ballistic"
	"sync"
)

type NewQueueFunc = func(model ballistic.DataModel) (ballistic.Queue, error)

func NewPool(newQueue NewQueueFunc) ballistic.Pool {
	return &Pool{
		newQueue:  newQueue,
		openQueue: map[string]ballistic.Queue{},
	}
}

type Pool struct {
	newQueue  NewQueueFunc
	ofsMx     sync.Mutex
	openQueue map[string]ballistic.Queue
}

func (p *Pool) getQueue(model ballistic.DataModel) (ballistic.Queue, error) {
	var err error
	queue, isInit := p.openQueue[model.SQL()]
	if !isInit {
		queue, err = p.newQueue(model)
		if err != nil {
			return nil, err
		}

		p.openQueue[model.SQL()] = queue
	}

	return queue, nil
}

func (p *Pool) Append(models []ballistic.DataModel) error {
	p.ofsMx.Lock()
	defer p.ofsMx.Unlock()

	for _, model := range models {
		queue, err := p.getQueue(model)
		if err != nil {
			return err
		}

		err = queue.Push(model)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Pool) Push(model ballistic.DataModel) (err error) {
	p.ofsMx.Lock()
	defer p.ofsMx.Unlock()

	queue, err := p.getQueue(model)
	if err != nil {
		return err
	}

	return queue.Push(model)
}

func (p *Pool) Eject(limit int) (models []ballistic.DataModel, err error) {
	p.ofsMx.Lock()
	defer p.ofsMx.Unlock()

	maxLimit := 0
	for _, queue := range p.openQueue {
		maxLimit += queue.Len()
	}

	if limit > maxLimit {
		limit = maxLimit
	}

	if limit < 0 {
		limit = maxLimit
	}

	if limit == 0 {
		return nil, nil
	}

	models = make([]ballistic.DataModel, 0, limit)
	for _, queue := range p.openQueue {
		ejectModels, err := queue.Eject(limit - len(models))
		if err != nil {
			return nil, err
		}

		for _, em := range ejectModels {
			if em != nil {
				models = append(models, em.(ballistic.DataModel))
			}
		}

		if len(models) >= limit {
			return models, nil
		}
	}
	return models, nil
}

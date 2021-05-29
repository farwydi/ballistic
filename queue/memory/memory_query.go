package memory

import (
	"container/list"
	"encoding"
	"sync"
)

func NewQueue() *Queue {
	return &Queue{
		buffer: list.New(),
	}
}

type Queue struct {
	buffer *list.List
	mx     sync.Mutex
}

func (m *Queue) Eject(limit int) (models []interface{}, err error) {
	m.mx.Lock()
	defer m.mx.Unlock()

	if limit > m.buffer.Len() {
		limit = m.buffer.Len()
	}

	if limit < 0 {
		limit = m.buffer.Len()
	}

	if limit == 0 {
		return nil, nil
	}

	models = make([]interface{}, 0, limit)
	it := 0
	for e := m.buffer.Front(); e != nil && it < limit; {
		cur := e
		e = e.Next()
		models = append(models, m.buffer.Remove(cur))
		it++
	}
	return models, nil
}

func (m *Queue) Push(model encoding.BinaryMarshaler) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.buffer.PushBack(model)
	return nil
}

func (m *Queue) Len() int {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.buffer.Len()
}

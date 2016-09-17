package transactional

import (
	"github.com/Preetam/lm2"

	"sync"
)

type Collection struct {
	col        *lm2.Collection
	writerLock sync.Mutex
}

func NewCollection(col *lm2.Collection) *Collection {
	return &Collection{
		col:        col,
		writerLock: sync.Mutex{},
	}
}

func (c *Collection) View(f func(*lm2.Cursor) error) error {
	cursor, err := c.col.NewCursor()
	if err != nil {
		return err
	}
	return f(cursor)
}

func (c *Collection) Update(f func(*lm2.Cursor, *lm2.WriteBatch) error) error {
	c.writerLock.Lock()
	defer c.writerLock.Unlock()

	cursor, err := c.col.NewCursor()
	if err != nil {
		return err
	}
	wb := lm2.NewWriteBatch()
	err = f(cursor, wb)
	if err != nil {
		return err
	}
	_, err = c.col.Update(wb)
	return err
}

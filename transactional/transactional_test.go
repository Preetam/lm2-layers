package transactional

import (
	"testing"

	"github.com/Preetam/lm2"
)

func TestTransactional1(t *testing.T) {
	col, err := lm2.NewCollection("/tmp/test_transactional_1.lm2", 10000)
	if err != nil {
		t.Fatal(err)
	}

	txCol := NewCollection(col)

	err = txCol.Update(func(cur *lm2.Cursor, wb *lm2.WriteBatch) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = txCol.View(func(cur *lm2.Cursor) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

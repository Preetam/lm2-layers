package transactional

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

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

func verifySquares(cur *lm2.Cursor) error {
	cur.Seek("a")
	ints := []int{}
	pairs := []string{}
	for cur.Next() {
		pairs = append(pairs, cur.Key()+" => "+cur.Value())
		val := cur.Value()
		n, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		ints = append(ints, n)
	}

	prev := 0
	for _, n := range ints {
		//log.Println(n)
		if prev == 0 {
			prev = n
			continue
		}
		if n != prev*prev {
			return fmt.Errorf("not a square: %v, %v", ints, pairs)
		}
		prev = n
	}

	return nil
}

func setSquares(wb *lm2.WriteBatch) {
	first := rand.Intn(1000)
	second := first * first
	third := second * second
	wb.Set("a", strconv.Itoa(first))
	wb.Set("b", strconv.Itoa(second))
	wb.Set("c", strconv.Itoa(third))
}

func TestTransactionalSquares(t *testing.T) {
	seed := time.Now().Unix()
	t.Log("seed:", seed)
	rand.Seed(seed)
	col, err := lm2.NewCollection("/tmp/test_transactional_squares.lm2", 10000)
	if err != nil {
		t.Fatal(err)
	}

	const count = 100
	const parallelism = 24

	txCol := NewCollection(col)

	wg := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				err = txCol.View(verifySquares)
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
	}

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				err = txCol.Update(func(cur *lm2.Cursor, wb *lm2.WriteBatch) error {
					setSquares(wb)
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
	}

	wg.Wait()
}

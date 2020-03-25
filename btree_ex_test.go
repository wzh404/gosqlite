package gosqlite_test

import (
	"fmt"
	"testing"

	"gosqlite"
)

func TestLoadFile(t *testing.T) {
	tree := gosqlite.LoadBtree("db0.log")
	tree.Print()
}

func TestBtree(t *testing.T) {
	tree := gosqlite.CreateTree(5)
	tree.Insert(5, []byte("val-5"))
	tree.Insert(2, []byte("val-222"))
	tree.Insert(15, []byte("val-1555"))
	tree.Insert(4, []byte("val-44444"))
	tree.Insert(7, []byte("val-7"))
	tree.Insert(9, []byte("val-9"))
	tree.Insert(19, []byte("val-19"))
	tree.Insert(11, []byte("val-11"))
	tree.Insert(1, []byte("val-1"))
	tree.Insert(32, []byte("val-32"))
	tree.Insert(21, []byte("val-21"))
	tree.Print()

	b := tree.Get(15)
	fmt.Printf("payload is [%s]\n", string(b))

	tree.RangeSearch(4, 15)
}

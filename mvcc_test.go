package gosqlite_test

import (
	"gosqlite"
	"testing"
)

func TestTrx(t *testing.T) {
	context := gosqlite.CreateTrxContext()
	trx1 := context.AllocteTrx()
	trx1.Begin(context)
	trx1.Insert(context, "trx1-data1")
	trx1.Select(context)
	trx1.Commit()

	trx2 := context.AllocteTrx()
	trx2.Begin(context)
	trx2.Insert(context, "trx2-data1")

	trx2.Select(context)
	trx2.Commit()
}

func TestTrx2(t *testing.T) {
	context := gosqlite.CreateTrxContext()
	trx1 := context.AllocteTrx()
	trx1.Begin(context)
	trx1.Insert(context, "trx1-data1")
	trx1.Select(context)
	trx1.Commit()

	trx2 := context.AllocteTrx()
	trx2.Begin(context)

	trx3 := context.AllocteTrx()
	trx3.Begin(context)

	trx3.Update(context, 1, "trx3-data0")
	trx2.Insert(context, "trx2-data1")
	trx3.Insert(context, "trx3-data1")

	trx2.Select(context)
	trx3.Select(context)

	trx2.Commit()
	trx3.Commit()
}

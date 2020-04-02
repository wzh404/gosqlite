package gosqlite

import (
	"fmt"
	"sync/atomic"
)

const (
	uncommit int8 = 1
	commit   int8 = 2
	unused   int8 = 0
	rollback int8 = 3
)

// TrxContext context
type TrxContext struct {
	trxIDs   []Trx
	dataPool []record
	undo     []record

	trxCounter int64
	rowCounter int64
}

// Trx be
type Trx struct {
	trxID  int64
	status int8
	view   *readView
}

type readView struct {
	lowLimitID int64
	upLimitID  int64
	trxIDs     []Trx
}

type record struct {
	rowID   int64
	trxID   int64
	rollPtr *record
	data    []byte
}

// CreateTrxContext to create trx context
func CreateTrxContext() *TrxContext {
	context := new(TrxContext)
	context.trxIDs = make([]Trx, 1024)
	context.dataPool = make([]record, 1024)
	context.undo = make([]record, 1024)
	for i := 0; i < len(context.trxIDs); i++ {
		context.trxIDs[i].trxID = 0
		context.trxIDs[i].status = unused
	}

	return context
}

// AllocteTrx to allocate trx from pool.
func (context *TrxContext) AllocteTrx() *Trx {
	for i := 0; i < len(context.trxIDs); i++ {
		if context.trxIDs[i].status == unused {
			return &context.trxIDs[i]
		}
	}

	return nil
}

// AllocteRecord to allocate trx from pool.
func (context *TrxContext) allocteUndo() *record {
	for i := 0; i < len(context.trxIDs); i++ {
		if context.undo[i].rowID == 0 {
			return &context.undo[i]
		}
	}

	return nil
}

// AllocteRecord to allocate trx from pool.
func (context *TrxContext) allocteRecord() *record {
	for i := 0; i < len(context.trxIDs); i++ {
		if context.dataPool[i].rowID == 0 {
			return &context.dataPool[i]
		}
	}

	return nil
}

func (context *TrxContext) findRecord(rowID int64) *record {
	for i := 0; i < len(context.trxIDs); i++ {
		if context.dataPool[i].rowID == rowID {
			return &context.dataPool[i]
		}
	}

	return nil
}

func (context *TrxContext) createReadView() *readView {
	view := new(readView)
	view.lowLimitID = 0
	view.upLimitID = 0

	ids := make([]Trx, 0)
	num := len(context.trxIDs)
	for i := 0; i < num; i++ {
		if context.trxIDs[i].status == uncommit {
			ids = append(ids, context.trxIDs[i])
		}
	}

	view.trxIDs = ids
	if len(view.trxIDs) > 0 {
		view.lowLimitID = view.trxIDs[0].trxID
		view.upLimitID = view.trxIDs[len(view.trxIDs)-1].trxID
	}
	return view
}

// Begin to trx
func (t *Trx) Begin(context *TrxContext) {
	atomic.AddInt64(&context.trxCounter, 1)
	t.trxID = context.trxCounter
	t.status = uncommit
	t.view = context.createReadView()

	fmt.Printf("begin trx %d\n", t.trxID)
}

// Commit to trx
func (t *Trx) Commit() {
	t.status = commit
	fmt.Printf("trx commit %d.\n", t.trxID)
}

// Rollback to trx
func (t *Trx) Rollback() {
	t.status = rollback
	fmt.Printf("trx rollback %d.\n", t.trxID)
}

// Insert to insert record
func (t *Trx) Insert(context *TrxContext, data string) {
	r := context.allocteRecord()
	r.data = []byte(data)
	r.trxID = t.trxID

	atomic.AddInt64(&context.rowCounter, 1)
	r.rowID = context.rowCounter
}

// Update to update record
func (t *Trx) Update(ctx *TrxContext, rowid int64, data string) {
	r := ctx.findRecord(rowid)
	u := ctx.allocteUndo()
	u.trxID = r.trxID
	u.data = r.data

	if r.rollPtr == nil {
		r.rollPtr = u
	} else {
		u1 := r.rollPtr
		r.rollPtr = u
		u.rollPtr = u1
	}

	r.trxID = t.trxID
	r.data = []byte(data)
}

func (t *Trx) inView(tid int64) bool {
	for i := 0; i < len(t.view.trxIDs); i++ {
		if t.view.trxIDs[i].trxID == tid {
			return true
		}
	}

	return false
}

func (t *Trx) check(tid int64) bool {
	if t.trxID == tid {
		return true
	}

	tmin := t.view.lowLimitID
	tmax := t.view.upLimitID

	if tid < tmin {
		return true
	} else if tid > tmax {
		return false
	} else if t.inView(tid) {
		return false
	}

	return true
}

func (t *Trx) selectRollback(ctx *TrxContext, r *record) {
	for p := r.rollPtr; p != nil; p = p.rollPtr {
		if p == nil {
			break
		}

		if t.check(p.trxID) {
			fmt.Printf("[%s] ", string(p.data))
			break
		}
	}
}

// Select to query trx data
func (t *Trx) Select(ctx *TrxContext) {
	fmt.Printf("\n**********%d*********\n", t.trxID)
	poolSize := len(ctx.dataPool)
	for i := 0; i < poolSize; i++ {
		if ctx.dataPool[i].rowID > 0 {
			tid := ctx.dataPool[i].trxID
			if t.check(tid) {
				fmt.Printf("%d:[%s] ", ctx.dataPool[i].rowID, string(ctx.dataPool[i].data))
			} else {
				t.selectRollback(ctx, &ctx.dataPool[i])
			}
		}
	}
	fmt.Printf("\n**********%d*********\n", t.trxID)
}

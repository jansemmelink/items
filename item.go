package items

import (
	"time"

	"github.com/jansemmelink/log"
)

//IItem in a table
type IItem interface {
	//table that the item belongs to
	Table() ITable
	NID() int
	UID() string

	//details about this revision
	Rev() IRev
	Data() IData

	//make and return the next revision
	Upd(data IData) (IItem, error)

	Del() error
}

type item struct {
	table ITable
	nid   int
	uid   string
	rev   IRev
	data  IData
}

//NewItem ...
func NewItem(table ITable, nid int, uid string, rev IRev, data IData) IItem {
	if table == nil || nid < 0 || len(uid) < 1 || rev.Nr() < 1 || data == nil {
		log.Errorf("NewItem(%p,%d,%s,{%d},%p)", table, nid, uid, rev.Nr(), data)
		return nil
	}
	return item{
		table: table,
		nid:   nid,
		uid:   uid,
		rev:   rev,
		data:  data,
	}
}

func (i item) Table() ITable {
	return i.table
}

func (i item) NID() int {
	return i.nid
}

func (i item) UID() string {
	return i.uid
}

func (i item) Rev() IRev {
	return i.rev
}

func (i item) Data() IData {
	return i.data
}

func (i item) Upd(data IData) (IItem, error) {
	//prepare the update using the next revision nr:
	updatedItem := item{
		table: i.table,
		nid:   i.nid,
		uid:   i.uid,
		rev:   rev{i.rev.Nr() + 1, time.Now()},
		data:  data,
	}

	//update in the table will fail if the item was already
	//at or beyond this next revision
	return i.table.UpdItem(updatedItem)
}

func (i item) Del() error {
	//prepare the old using the next revision nr:
	deletedItem := item{
		table: i.table,
		nid:   i.nid,
		uid:   i.uid,
		rev:   rev{i.rev.Nr() + 1, time.Now()},
		data:  i.data,
	}

	//delete in the table will fail if the item was already
	//at or beyond this next revision
	return i.table.DelItem(deletedItem)
}

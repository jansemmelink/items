package items

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

//ITable of items with the same structure
type ITable interface {
	//table description
	Name() string
	Type() reflect.Type
	Schema() ISchema
	Count() int

	//add a new item (with rev 1) to the table
	AddItem(data IData) (IItem, error)

	//upd will fail if item does not exist already with specified item.rev-1
	//it returns upd,nil on success, or nil,err if cannot update
	UpdItem(upd IItem) (IItem, error)

	//get the latest revision of the specified item
	GetItem(uid string) IItem

	//delete all revisions of the specified item (fail if not the latest revision anymore)
	DelItem(i IItem) error

	//get a list of all items at their current latest revision with uid as map index
	Items() map[string]IItem
}

//Table calls NewTable() and panics on error
func Table(name string, tmplStruct IData) ITable {
	t, err := NewTable(name, tmplStruct)
	if err != nil {
		panic(errors.Wrapf(err, "failed to create table"))
	}
	return t
}

//NewTable to store items using the same struct as specified in tmplStruct
func NewTable(name string, tmplStruct IData) (ITable, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, errors.Wrapf(err, "invalid table name=\"%s\"", name)
	}

	schema, err := NewSchema(reflect.TypeOf(tmplStruct))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot make schema of type %T", tmplStruct)
	}

	return &table{
		name:       name,
		structType: reflect.TypeOf(tmplStruct),
		schema:     schema,
		nextID:     1,
		items:      make(map[string]IItem),
	}, nil
}

//table implements ITable
type table struct {
	mutex      sync.Mutex
	name       string
	structType reflect.Type
	schema     ISchema
	nextID     int
	items      map[string]IItem
}

func (t *table) Name() string {
	if t == nil {
		panic("nil.Name()")
	}
	return t.name
}

func (t *table) Type() reflect.Type {
	if t == nil {
		panic("nil.Type()")
	}
	return t.structType
}

func (t *table) Schema() ISchema {
	if t == nil {
		panic("nil.Schema()")
	}
	return t.schema
}

func (t *table) Count() int {
	if t == nil {
		panic("nil.Count()")
	}
	return len(t.items)
}

func (t *table) AddItem(data IData) (IItem, error) {
	if t == nil {
		return nil, fmt.Errorf("nil.AddItem()")
	}
	if data == nil {
		return nil, fmt.Errorf("%s.AddItem(nil)", t.name)
	}
	if err := data.Validate(); err != nil {
		return nil, errors.Wrapf(err, "invalid %v data", t.structType)
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	//todo: check duplicate keys...

	newItem := NewItem(t, t.nextID, uuid.NewV1().String(), rev{nr: 1, ts: time.Now()}, data)
	t.items[newItem.UID()] = newItem
	t.nextID++
	return newItem, nil
}

func (t *table) UpdItem(upd IItem) (IItem, error) {
	if t == nil {
		return nil, fmt.Errorf("nil.UpdItem()")
	}

	//check table reference
	if upd.Table() != t {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) from other table(%s)", t.Name(), upd.NID(), upd.UID(), upd.Table().Name())
	}
	//check valid rev nr
	if upd.Rev().Nr() <= 1 {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) with rev.nr=%d should be >1", t.Name(), upd.NID(), upd.UID(), upd.Rev().Nr())
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	//get current revision of existing item
	cur, ok := t.items[upd.UID()]
	if !ok {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) not found", t.name, upd.NID(), upd.UID())
	}
	if cur.NID() != upd.NID() || cur.UID() != upd.UID() {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) != CurItem(%d,%s)", t.name, upd.NID(), upd.UID(), cur.NID(), cur.UID())
	}

	//make sure this will be the next rev
	if upd.Rev().Nr() != cur.Rev().Nr()+1 {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s).Rev.Nr=%d should be %d", t.name, upd.NID(), upd.UID(), upd.Rev().Nr(), cur.Rev().Nr()+1)
	}

	//correct: replace
	t.items[upd.UID()] = upd
	return upd, nil
}

func (t *table) GetItem(uid string) IItem {
	if t == nil {
		panic("nil.GetItem()")
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if existing, ok := t.items[uid]; ok {
		return existing
	}
	return nil
}

func (t *table) DelItem(old IItem) error {
	if t == nil {
		return fmt.Errorf("nil.DelItem()")
	}

	if old.Table() != t {
		return fmt.Errorf("%s.DelItem(nid=%d,uid=%s) from other table=%s", t.name, old.NID(), old.UID(), old.Table().Name())
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	//get current revision of existing item
	cur, ok := t.items[old.UID()]
	if !ok {
		return fmt.Errorf("%s.DelItem(%d,%s) not found", t.name, old.NID(), old.UID())
	}
	if cur.NID() != old.NID() || cur.UID() != old.UID() {
		return fmt.Errorf("%s.DelItem(%d,%s) != CurItem(%d,%s)", t.name, old.NID(), old.UID(), cur.NID(), cur.UID())
	}

	//make sure this is the current rev
	if old.Rev().Nr() != cur.Rev().Nr() {
		return fmt.Errorf("%s.DelItem(%d,%s).Rev.Nr=%d != CurItem().Rev.Nr=%d", t.name, old.NID(), old.UID(), cur.Rev().Nr(), cur.Rev().Nr())
	}

	//correct: delete
	delete(t.items, old.UID())
	return nil
}

func (t *table) Items() map[string]IItem {
	return t.items
}

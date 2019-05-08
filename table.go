package items

import (
	"fmt"
	"reflect"
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

	//delete all entries (currently: without keeping revisions, so complete wipe)
	DelAll() error

	Index(name string, fields []string) (IIndex, error)
}

//table implements ITable
type table struct {
	db         IDb
	name       string
	structType reflect.Type
	schema     ISchema
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

func (t *table) AddItem(data IData) (IItem, error) {
	return nil, fmt.Errorf("db(%s).table(%s).AddItem() not implemented", t.db.Name(), t.name)
}

func (t *table) UpdItem(upd IItem) (IItem, error) {
	return nil, fmt.Errorf("db(%s).table(%s).UpdItem() not implemented", t.db.Name(), t.name)
}

func (t *table) GetItem(uid string) IItem {
	return nil //, fmt.Errorf("db(%s).table(%s).GetItem() not implemented", t.db.Name(), t.name)
}

func (t *table) DelItem(old IItem) error {
	return fmt.Errorf("db(%s).table(%s).DelItem() not implemented", t.db.Name(), t.name)
}

func (t *table) Items() map[string]IItem {
	return make(map[string]IItem)
}

func (t *table) DelAll() error {
	return fmt.Errorf("db(%s).table(%s).DelAll() not implemented", t.db.Name(), t.name)
}

func (t *table) Index(name string, fields []string) (IIndex, error) {
	return nil, fmt.Errorf("db(%s).table(%T:%s).Index() not implemented", t.db.Name(), t, t.name)
}

func (t *table) Count() int {
	if t == nil {
		panic("nil.Count()")
	}
	return -1
	//return len(t.items)
}

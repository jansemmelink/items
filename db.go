package items

import (
	"fmt"
	"reflect"
	"sync"

	//	"github.com/jansemmelink/log"
	"github.com/pkg/errors"
)

//IDb to store tables of items
type IDb interface {
	Name() string

	Table(name string, tmplStruct IData) (ITable, error)
	MustTable(name string, tmplStruct IData) ITable
	RemTable(t ITable)
	GetTable(name string) ITable
	Tables() map[string]ITable
}

//New database
func New(name string) IDb {
	return &Database{
		name:   name,
		tables: make(map[string]ITable),
	}
}

//Database implements IDb and can be embedded into user database types
//e.g. to implement a SQL persistent database
type Database struct {
	mutex  sync.Mutex
	name   string
	tables map[string]ITable
}

//Name ...
func (d *Database) Name() string {
	return d.name
}

//Table ...
func (d *Database) Table(name string, tmplStruct IData) (ITable, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, errors.Wrapf(err, "Database(%s).table(%s) invalid name", d.name, name)
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()
	if _, ok := d.tables[name]; ok {
		return nil, fmt.Errorf("Database(%s).table(%s) already exists", d.name, name)
	}

	schema, err := NewSchema(reflect.TypeOf(tmplStruct))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot make schema of type %T", tmplStruct)
	}

	t := &table{
		db:         d,
		name:       name,
		structType: reflect.TypeOf(tmplStruct),
		schema:     schema,
	}
	d.tables[t.Name()] = t
	return t, nil
}

//MustTable ...
func (d *Database) MustTable(name string, tmplStruct IData) ITable {
	t, err := d.Table(name, tmplStruct)
	if err != nil {
		panic(errors.Wrapf(err, "db(%s).table(%s) failed", d.name, name))
	}
	return t
}

//RemTable removes the table
func (d *Database) RemTable(t ITable) {
	if d != nil && t != nil {
		delete(d.tables, t.Name())
	}
}

//GetTable ...
func (d *Database) GetTable(name string) ITable {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	tbl, _ := d.tables[name]
	return tbl
}

//Tables ...
func (d *Database) Tables() map[string]ITable {
	return d.tables
}

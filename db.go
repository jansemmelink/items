package items

import (
	"fmt"
	"sync"

	"github.com/jansemmelink/log"
)

//IDb to store tables of items
type IDb interface {
	Name() string
	AddTable(t ITable) (ITable, error)
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

//AddTable ...
func (d *Database) AddTable(t ITable) (ITable, error) {
	log.Debugf("Database.AddTable()")
	if d == nil {
		return nil, fmt.Errorf("nil.AddTable()")
	}
	if t == nil {
		return nil, fmt.Errorf("Database(%s).AddTable(nil)", d.name)
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, ok := d.tables[t.Name()]; ok {
		return nil, fmt.Errorf("Database(%s).table(%s) already exists", d.name, t.Name())
	}
	d.tables[t.Name()] = t
	return t, nil
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

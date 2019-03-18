package mem

import (
	"github.com/jansemmelink/items"
)

//New creates a new in-memory database
//note: name is optional
func New(name string) (items.IDb, error) {
	return &memDatabase{
		IDb: items.New(name),
	}, nil
}

//memDatabase extends the default items.Database to store in memory
type memDatabase struct {
	items.IDb
}

func (db *memDatabase) Table(name string, tmplStruct items.IData) (items.ITable, error) {
	//add the table to the db
	it, err := db.IDb.Table(name, tmplStruct)
	if err != nil {
		return nil, err
	}

	//describe the table
	return &memTable{
		ITable: it,
		nextID: 1,
		items:  make(map[string]items.IItem),
	}, nil
}

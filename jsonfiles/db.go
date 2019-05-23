package jsonfiles

import (
	"path"

	"github.com/jansemmelink/items"
	"github.com/jansemmelink/log"
)

//New creates a new database that stores item data in JSON files in a directory tree
func New(dir string) (items.IDb, error) {
	temp := dir
	name := ""
	for len(name) == 0 {
		name = path.Base(temp)
		temp = path.Dir(temp)
	}
	if err := Mkdir(dir); err != nil {
		return nil, log.Wrapf(err, "failed to make db dir=%s", dir)
	}
	return &db{
		IDb: items.New(name),
		dir: dir,
	}, nil
}

//db extends the default items.Database to store in JSON files
type db struct {
	items.IDb
	dir string
}

func (db *db) Table(name string, tmplStruct items.IData) (items.ITable, error) {
	//add the table to the db
	it, err := db.IDb.Table(name, tmplStruct)
	if err != nil {
		return nil, err
	}

	//describe the table
	return newTable(it, db.dir), nil
}

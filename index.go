package items

import "reflect"

//IIndex on a selection of table fields
type IIndex interface {
	Table() ITable
	Fields() []string
	FindOne() (IItem, error)
	Find() ([]IItem, error)
}

type index struct {
	table  ITable
	fields []indexField
}

type indexField struct {
	name        string
	structField reflect.StructField
}

func (i index) Table() ITable {
	return i.table
}

func (i index) Fields() []string {
	f := make([]string, 0)
	for _, sf := range i.fields {
		f = append(f, sf.name)
	}
	return f
}

func (i index) FindOne() (IItem, error) {
	return nil, nil
}

func (i index) Find() ([]IItem, error) {
	return nil, nil
}

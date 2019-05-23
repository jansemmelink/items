package items

import (
	"fmt"
	"reflect"

	//"github.com/jansemmelink/log"
	"github.com/pkg/errors"
)

//IIndex on a selection of table fields
type IIndex interface {
	Table() ITable
	Name() string
	Fields() []string
	ItemKey(IItem) IKey
	MapKey(m map[string]interface{}) IKey
	Add(IItem) error
	FindOne(key map[string]interface{}) (IItem, error)
	Find(key map[string]interface{}) ([]IItem, error)
}

type index struct {
	table  ITable
	name   string
	fields []indexField
}

//NewIndex definition
func NewIndex(t ITable, name string, fieldNames []string) (IIndex, error) {
	if t == nil {
		panic("NewIndex(t==nil)")
	}
	if err := validateIdentifier(name); err != nil {
		panic(errors.Wrapf(err, "Invalid index name=\"%s\"", name))
	}
	if len(fieldNames) == 0 {
		panic("NewIndex(t,no fields)")
	}

	i := index{
		table:  t,
		name:   name,
		fields: make([]indexField, 0),
	}

	//make sure fields are unique and defined in the table struct type
	for _, fn := range fieldNames {
		for _, iField := range i.fields {
			if iField.name == fn {
				return nil, fmt.Errorf("duplicate index field %s on table %s", fn, t.Name())
			}
		}
		structField, ok := t.Type().FieldByName(fn)
		if !ok {
			return nil, fmt.Errorf("table %s does not have field %s to use in index", t.Name(), fn)
		}
		i.fields = append(i.fields, indexField{
			name:        fn,
			index:       structField.Index[0],
			structField: structField,
		})
	}
	return i, nil
}

type indexField struct {
	name        string
	index       int
	structField reflect.StructField
}

func (i index) Table() ITable {
	return i.table
}

func (i index) Name() string {
	return i.name
}

func (i index) Fields() []string {
	f := make([]string, 0)
	for _, sf := range i.fields {
		f = append(f, sf.name)
	}
	return f
}

func (i index) ItemKey(item IItem) IKey {
	itemData := item.Data()
	itemDataValue := reflect.ValueOf(itemData)
	key := NewKey()
	for _, f := range i.fields {
		keyValue := itemDataValue.Field(f.index).Interface()
		key = key.With(f.name, keyValue)
		//log.Debugf("key(%s)=%v", f.name, keyValue)
	}
	return key
}

func (i index) MapKey(m map[string]interface{}) IKey {
	key := NewKey()
	for _, f := range i.fields {
		keyValue := m[f.name]
		key = key.With(f.name, keyValue)
		//log.Debugf("key(%s)=%v", f.name, keyValue)
	}
	return key
}

func (i index) Add(IItem) error {
	return fmt.Errorf("Index(%s).Add not implemented", i.Name())
}

func (i index) FindOne(key map[string]interface{}) (IItem, error) {
	return nil, fmt.Errorf("Index(%T:%s).FindOne not implemented", i, i.Name())
}

func (i index) Find(key map[string]interface{}) ([]IItem, error) {
	return nil, fmt.Errorf("Index(%s).Find not implemented", i.Name())
}

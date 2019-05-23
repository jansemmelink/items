package jsonfiles

import (
	"fmt"

	"github.com/jansemmelink/items"
	"github.com/jansemmelink/log"
)

type index struct {
	items.IIndex
	item map[string]items.IItem
}

func (i *index) Add(item items.IItem) error {
	if i == nil {
		return fmt.Errorf("nil.Add()")
	}
	if item == nil {
		return fmt.Errorf("index(%s).Add(nil)", i.Name())
	}

	//make key string
	keyString := i.ItemKey(item).String()
	if _, ok := i.item[keyString]; ok {
		return fmt.Errorf("duplicate key %s", keyString)
	}
	i.item[keyString] = item
	return nil
}

func (i index) FindOne(key map[string]interface{}) (items.IItem, error) {
	log.Debugf("Finding in list of %d items", len(i.item))
	keyString := i.MapKey(key).String()
	if item, ok := i.item[keyString]; ok {
		return item, nil
	}
	return nil, nil
}

func (i index) Find(key map[string]interface{}) ([]items.IItem, error) {
	return nil, fmt.Errorf("Index(%s).Find not implemented", i.Name())
}

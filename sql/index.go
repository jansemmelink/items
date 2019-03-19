package sql

import (
	"fmt"
	"reflect"
	"time"

	"github.com/jansemmelink/items"
	"github.com/jansemmelink/log"
	"github.com/pkg/errors"
)

type sqlIndex struct {
	items.IIndex
	item map[string]items.IItem
}

func (i *sqlIndex) Add(item items.IItem) error {
	//db will take care of this
	return nil
}

func (i *sqlIndex) FindOne(key map[string]interface{}) (items.IItem, error) {
	//find in tbl, sql will use the index
	if i == nil || key == nil {
		return nil, fmt.Errorf("sqlIndex.FindOne()")
	}

	t := i.Table().(*sqlTable)

	//get only the latest revNr for the matching key:
	queryStr := fmt.Sprintf("SELECT nid,uid,revNr,revTs,%s FROM `%s`", t.csvFieldNames, t.tableName)

	keyString := ""
	for n, v := range key {
		keyString += fmt.Sprintf(" AND %s=\"%v\"", n, v)
		//todo: multi-field index will need "AND" connector
		//todo: other data types does not need quotes etc...
	}
	queryStr += fmt.Sprintf(" WHERE %s", keyString[5:]) //skip over first " AND "

	queryStr += fmt.Sprintf(" ORDER BY revNr DESC LIMIT 1")
	rows, err := t.conn.Query(queryStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %s.(%+v): sql=%s: %v", t.Name(), key, queryStr, err)
	}

	if !rows.Next() {
		log.Debugf("%s.(%+v) not found", t.Name(), key)
		return nil, nil
	}

	itemDataPtrValue := reflect.New(t.Type())
	itemData := itemDataPtrValue.Interface().(items.IData)
	var nid int
	var uid string
	var revNr int
	var revTsString string
	values := append([]interface{}{&nid, &uid, &revNr, &revTsString}, itemValues(itemData)...)
	if err = rows.Scan(values...); err != nil {
		return nil, errors.Wrapf(err, "failed to parse SQL row into %v: %v", t.Type(), err)
	}

	//if revTsString ends with ".DEL", the item was deleted
	if revTsString[14:] == ".DEL" {
		return nil, nil
	}

	revTs, err := time.Parse(revTsFormat, revTsString)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse revTs=%s into %v: %v", revTsString, revTsFormat, err)
	}
	log.Debugf("Parsed %s.nid=%d,uid=%s: %+v", t.Name(), nid, uid, itemData)

	//dereference the itemData to return the struct, not a pointer to the struct:
	return items.NewItem(t, nid, uid, items.Rev(revNr, revTs), itemDataPtrValue.Elem().Interface().(items.IData)), nil
}

func (i sqlIndex) Find(key map[string]interface{}) ([]items.IItem, error) {
	return nil, fmt.Errorf("Index(%s).Find not implemented", i.Name())
}

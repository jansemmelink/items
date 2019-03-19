package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/jansemmelink/items"
	"github.com/jansemmelink/log"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type sqlTable struct {
	items.ITable
	conn          *sql.DB
	tableName     string
	csvFieldNames string
}

const revTsFormat = "20060102150405.000"

func (t *sqlTable) Count() int {
	if t == nil {
		return 0
	}

	queryStr := fmt.Sprintf("SELECT COUNT(*) FROM `%s` GROUP BY uid", t.tableName)
	rows, err := t.conn.Query(queryStr)
	if err != nil {
		log.Errorf("Failed to count %s with: %s", t.Name(), queryStr)
		return 0
	}
	if !rows.Next() {
		log.Errorf("No row from counting %s with: %s", t.Name(), queryStr)
		return 0
	}

	var count int
	if err = rows.Scan(&count); err != nil {
		log.Errorf("Failed to parse count: %v", err)
		return 0
	}
	return count
}

func (t *sqlTable) AddItem(itemData items.IData) (items.IItem, error) {
	if t == nil {
		return nil, fmt.Errorf("nil.AddItem()")
	}
	if itemData == nil {
		return nil, fmt.Errorf("%s.AddItem(nil)", t.Name())
	}
	if err := itemData.Validate(); err != nil {
		return nil, errors.Wrapf(err, "invalid %v data", t.Type())
	}

	//we try to insert the item into the SQL table
	//and let SQL assign the incrementing ID, while we assign the uid here
	uid := uuid.NewV1().String()
	rev := items.Rev(1, time.Now())
	values, err := itemValueDef(itemData)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to define %T values for SQL", itemData)
	}

	queryStr := fmt.Sprintf("INSERT INTO `%s` SET uid=\"%s\",revNr=%d,revTs=\"%s\",%s", t.tableName, uid, rev.Nr(), rev.Timestamp().UTC().Format(revTsFormat), values)
	result, err := t.conn.Exec(queryStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to insert %T with: %s", itemData, queryStr)
		//todo: check duplicate keys... and other failures...
		//e.g. mark user.name must be unique...
	}

	nid, err := result.LastInsertId()
	newItem := items.NewItem(t, int(nid), uid, rev, itemData)

	return newItem, nil
	//return t.ITable.AddItem(data)
} //sqlTable.AddItem()

func (t *sqlTable) UpdItem(upd items.IItem) (items.IItem, error) {
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

	values, err := itemValueDef(upd.Data())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to define %T values for SQL", upd.Data())
	}

	//update is another insert with the next rev nr
	//the rev nr is incremented by IITem before calling this
	//and this insert will fail on duplicate key if the next rev nr is already used
	//in that case, you need to get again to get the latest changes made by someone else, and then upd again
	queryStr := fmt.Sprintf("INSERT INTO `%s` SET", t.tableName)
	queryStr += fmt.Sprintf(" uid=\"%s\"", upd.UID())
	queryStr += fmt.Sprintf(",revNr=%d,revTs=\"%s\"", upd.Rev().Nr(), upd.Rev().Timestamp().UTC().Format(revTsFormat))
	queryStr += "," + values

	result, err := t.conn.Exec(queryStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to insert %s with: %s", t.Name(), queryStr)
	}

	nid, err := result.LastInsertId()
	newItem := items.NewItem(t, int(nid), upd.UID(), upd.Rev(), upd.Data())
	return newItem, nil
} //sqlTable.UpdItem()

func (t *sqlTable) GetItem(uid string) items.IItem {
	if t == nil {
		panic("nil.GetItem()")
	}

	//get only the latest revNr:
	queryStr := fmt.Sprintf("SELECT nid,revNr,revTs,%s FROM `%s` WHERE uid=\"%s\" ORDER BY revNr DESC LIMIT 1", t.csvFieldNames, t.tableName, uid)
	rows, err := t.conn.Query(queryStr)
	if err != nil {
		log.Debugf("ERROR: failed to get %s.uid=%s: sql=%s: %v", t.Name(), uid, queryStr, err)
		return nil
	}

	if !rows.Next() {
		log.Debugf("%s.uid=%s not found", t.Name(), uid)
		return nil
	}

	itemData := reflect.New(t.Type()).Interface().(items.IData)
	var nid int
	var revNr int
	var revTsString string
	values := append([]interface{}{&nid, &revNr, &revTsString}, itemValues(itemData)...)
	if err = rows.Scan(values...); err != nil {
		log.Debugf("ERROR: failed to parse SQL row into %v: %v", t.Type(), err)
		return nil
	}

	//if revTsString ends with ".DEL", the item was deleted
	if revTsString[14:] == ".DEL" {
		return nil
	}

	revTs, err := time.Parse(revTsFormat, revTsString)
	if err != nil {
		log.Errorf("ERROR: failed to parse revTs=%s into %v: %v", revTsString, revTsFormat, err)
		return nil
	}
	log.Debugf("Parsed %s.nid=%d,uid=%s: %+v", t.Name(), nid, uid, itemData)
	return items.NewItem(t, nid, uid, items.Rev(revNr, revTs), itemData)
} //sqlTable.GetItem()

func (t *sqlTable) DelItem(old items.IItem) error {
	if t == nil {
		return fmt.Errorf("nil.DelItem()")
	}
	if old.Table() != t {
		return fmt.Errorf("%s.DelItem(nid=%d,uid=%s) from other table=%s", t.Name(), old.NID(), old.UID(), old.Table().Name())
	}

	//mark as deleted by changing the last 3 digits of timestamp to be "DEL"
	delTs := old.Rev().Timestamp().UTC().Format(revTsFormat)
	delTs = delTs[0:14] + ".DEL"

	values, err := itemValueDef(old.Data())
	if err != nil {
		return errors.Wrapf(err, "failed to define %s values for SQL", t.Name())
	}

	//delete by inserting new record with next rev nr
	//marked as deleted. It will fail if done with an old rev, not the latest
	queryStr := fmt.Sprintf("INSERT INTO `%s` SET", t.tableName)
	queryStr += fmt.Sprintf(" uid=\"%s\"", old.UID())
	queryStr += fmt.Sprintf(",revNr=%d,revTs=\"%s\"", old.Rev().Nr(), delTs)
	queryStr += "," + values
	_, err = t.conn.Exec(queryStr)
	if err != nil {
		return errors.Wrapf(err, "failed to mark %s as deleted with: %s", t.Name(), queryStr)
	}
	return nil
} //sqlTable.DelItem()

func (t *sqlTable) DelAll() error {
	if t == nil {
		return fmt.Errorf("nil.DelAll()")
	}

	//TODO: Does not preserve history - need to insert individuals to be complient!
	queryStr := fmt.Sprintf("DELETE FROM `%s`", t.tableName)
	_, err := t.conn.Exec(queryStr)
	if err != nil {
		return errors.Wrapf(err, "failed to deleted all from %s", t.Name())
	}
	return nil
}

func (t *sqlTable) Index(name string, fieldNames []string) (items.IIndex, error) {
	//for now just return because mysql will find on any field without an index
	//but this must be created soon to improve performance on large tables
	//todo!
	newIndex, err := items.NewIndex(t, name, fieldNames)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to describe index")
	}

	//add the index to the table
	si := &sqlIndex{
		IIndex: newIndex,
	}
	return si, nil
}

func itemValueDef(i interface{}) (string, error) {
	log.Debugf("itemValueDef(%T)", i)
	t := reflect.TypeOf(i)
	v := reflect.ValueOf(i)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return "", fmt.Errorf("itemValueDef(%T) is not a struct", i)
	}

	valueDef := ""
	log.Debugf("  %T has %d fields", i, v.NumField())
	for fieldIndex := 0; fieldIndex < v.NumField(); fieldIndex++ {
		fieldValue := v.Field(fieldIndex)
		fieldType := t.Field(fieldIndex)
		if !fieldValue.CanInterface() {
			log.Debugf("  %T.%s cannot be accessed - skip", i, fieldType.Name)
			continue
		}
		log.Debugf("Field[%d]: %+v", fieldIndex, fieldValue)

		switch fieldType.Type.Kind() {
		case reflect.Int, reflect.Float32, reflect.Float64, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			//numeric values are not quoted:
			valueDef += fmt.Sprintf(",%s=%v", fieldType.Name, fieldValue.Interface())
		case reflect.Struct:
			switch fieldType.Type {
			case reflect.TypeOf(time.Time{}):
				//time value format
				valueDef += fmt.Sprintf(",%s=\"%s\"", fieldType.Name, fieldValue.Interface().(time.Time).UTC().Format("2006-01-02 15:04:05.000000"))
			default:
				//default to some quoted value
				//consider encoding JSON here for structs
				valueStr := fmt.Sprintf("%v", fieldValue.Interface())
				valueDef += fmt.Sprintf(",%s=\"%s\"", fieldType.Name, escape(valueStr))
			}
		default:
			//default to some quoted value
			valueStr := fmt.Sprintf("%v", fieldValue.Interface())
			valueDef += fmt.Sprintf(",%s=\"%v\"", fieldType.Name, escape(valueStr))
		}
	}

	if len(valueDef) == 0 {
		log.Debugf("itemValueDef(%T) -> \"\"", i)
		return "", nil
	}

	log.Debugf("itemValueDef(%T) -> %s", i, valueDef[1:])
	return valueDef[1:], nil
}

//escape is elementary assuming mysql - need to extend to consider other SQL drivers
//e.g. PostgreSQL and Microsoft etc...
func escape(source string) string {
	j := 0
	if len(source) == 0 {
		return ""
	}
	tempStr := source[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\r':
			flag = true
			escape = '\r'
			break
		case '\n':
			flag = true
			escape = '\n'
			break
		case '\\':
			flag = true
			escape = '\\'
			break
		case '\'':
			flag = true
			escape = '\''
			break
		case '"':
			flag = true
			escape = '"'
			break
		case '\032':
			flag = true
			escape = 'Z'
			break
		default:
		}
		if flag {
			desc[j] = '\\'
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}
	log.Debugf("Escaped \"%s\" -> \"%s\"", source, desc[0:j])
	return string(desc[0:j])
}

//itemValues returns an array of pointers to fields in the item
//that can be populated with sql query result Scan()
//in the same order as itemFields
func itemValues(i items.IData) []interface{} {
	//add pointer to each field into list for scanning the SQL result
	//hard coded, it would look like this:
	//err := rows.Scan(&bk.Isbn, &bk.Title, &bk.Author, &bk.Price)
	//or like this:
	// var Isbn string
	// var Title string
	// var Author string
	// var Price float32
	// values = append(values, &Isbn, &Title, &Author, &Price)
	//but we get it from reflect:
	values := make([]interface{}, 0)
	v := reflect.ValueOf(i).Elem()
	//t := reflect.TypeOf(i).Elem()
	for fieldIndex := 0; fieldIndex < v.NumField(); fieldIndex++ {
		fieldValue := v.Field(fieldIndex)
		//fieldType := t.Field(fieldIndex)
		if fieldValue.CanSet() {
			//log.Debugf("Adding field %s...", fieldType.Name)
			values = append(values, fieldValue.Addr().Interface())
		}
	}
	return values
}

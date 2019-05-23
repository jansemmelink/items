package jsonfiles

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/jansemmelink/items"
	"github.com/jansemmelink/log"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

func newTable(it items.ITable, dbDir string) *table {
	t := &table{
		ITable: it,
		dir:    dbDir + "/" + it.Name(),
		nextID: 1,
		index:  make(map[string]items.IIndex),
	}

	if err := Mkdir(t.dir); err != nil {
		return nil
	}

	t.filenamePattern = it.Name() + `_([a-zA-Z0-9-]+).json`
	t.filenameAllPattern = it.Name() + `(deleted)*_([a-zA-Z0-9-]+)(_r([0-9]+))*.json`

	var err error
	t.filenameRegex, err = regexp.Compile(t.filenamePattern)
	if err != nil {
		panic("Failed to compile table filename pattern: " + err.Error())
	}
	t.filenameAllRegex, err = regexp.Compile(t.filenameAllPattern)
	if err != nil {
		panic("Failed to compile table filenameWithRev pattern: " + err.Error())
	}
	return t
}

type table struct {
	items.ITable
	nextID int
	dir    string

	filenamePattern string
	filenameRegex   *regexp.Regexp

	filenameAllPattern string
	filenameAllRegex   *regexp.Regexp
	index              map[string]items.IIndex
}

func (t *table) Count() int {
	if t == nil {
		return 0
	}

	//count the nr of files in the table directory
	count := 0
	err := filepath.Walk(t.dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if t.filenameRegex.MatchString(path) {
			count++
		}
		return nil
	})
	if err != nil {
		return 0
	}
	return count
}

func (t *table) AddItem(data items.IData) (items.IItem, error) {
	if t == nil {
		return nil, fmt.Errorf("nil.AddItem()")
	}
	if data == nil {
		return nil, fmt.Errorf("%s.AddItem(nil)", t.Name())
	}
	if err := data.Validate(); err != nil {
		return nil, errors.Wrapf(err, "invalid %v data", t.Type())
	}

	newItem := items.NewItem(t, t.nextID, uuid.NewV1().String(), items.Rev(1, time.Now()), data)
	for indexName, index := range t.index {
		if err := index.Add(newItem); err != nil {
			return nil, errors.Wrapf(err, "cannot add to index %s", indexName)
		}
	}

	//save to file will create a new file
	if err := t.writeItem(newItem); err != nil {
		return nil, log.Wrapf(err, "failed to write new item to file")
	}
	t.nextID++
	return newItem, nil
}

func (t *table) UpdItem(upd items.IItem) (items.IItem, error) {
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

	//get current revision of existing item
	cur := t.GetItem(upd.UID())
	if cur == nil {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) not found", t.Name(), upd.NID(), upd.UID())
	}
	if cur.NID() != upd.NID() || cur.UID() != upd.UID() {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) != CurItem(%d,%s)", t.Name(), upd.NID(), upd.UID(), cur.NID(), cur.UID())
	}

	//make sure this will be the next rev
	if upd.Rev().Nr() != cur.Rev().Nr()+1 {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s).Rev.Nr=%d should be %d", t.Name(), upd.NID(), upd.UID(), upd.Rev().Nr(), cur.Rev().Nr()+1)
	}

	//correct: copy current file then overwrite
	if err := CopyFile(
		t.itemFilename(upd.NID(), upd.UID(), 0),                            //src file has no rev nr
		t.itemFilename(upd.NID(), upd.UID(), upd.Rev().Nr())); err != nil { //dst file has rev nr
		return nil, log.Wrapf(err, "failed to backup %s.uid=%s rev %d", t.ITable.Name(), upd.UID(), upd.Rev().Nr())
	}

	if err := t.writeItem(upd); err != nil {
		return nil, log.Wrapf(err, "failed to write update to file")
	}
	return upd, nil
}

func (t *table) GetItem(uid string) items.IItem {
	if t == nil {
		panic("nil.GetItem()")
	}

	fn := t.itemFilename(0, uid, 0)
	item, err := t.readFile(fn)
	if err != nil {
		return nil
	}
	return item
}

func (t *table) DelItem(old items.IItem) error {
	if t == nil {
		return fmt.Errorf("nil.DelItem()")
	}

	if old.Table() != t {
		return fmt.Errorf("%s.DelItem(nid=%d,uid=%s) from other table=%s", t.Name(), old.NID(), old.UID(), old.Table().Name())
	}

	//get current revision of existing item
	cur := t.GetItem(old.UID())
	if cur == nil {
		return fmt.Errorf("%s.DelItem(%d,%s) not found", t.Name(), old.NID(), old.UID())
	}
	if cur.NID() != old.NID() || cur.UID() != old.UID() {
		return fmt.Errorf("%s.DelItem(%d,%s) != CurItem(%d,%s)", t.Name(), old.NID(), old.UID(), cur.NID(), cur.UID())
	}

	//make sure this will be the next rev
	if old.Rev().Nr() != cur.Rev().Nr()+1 {
		return fmt.Errorf("%s.DelItem(%d,%s).Rev.Nr=%d should be %d", t.Name(), old.NID(), old.UID(), old.Rev().Nr(), cur.Rev().Nr()+1)
	}
	//correct: delete
	if err := os.Rename(
		t.itemFilename(old.NID(), old.UID(), 0),
		t.itemFilename(old.NID(), old.UID(), -1)); err != nil {
		return log.Wrapf(err, "failed to rename file to mark item as deleted")
	}

	return nil
}

func (t *table) Items() map[string]items.IItem {
	list := make(map[string]items.IItem)
	for _, fn := range t.itemFilenames() {
		if item, err := t.readFile(fn); err == nil {
			list[item.UID()] = item
		}
	}
	return list
}

func (t *table) DelAll() error {
	for _, fn := range t.itemAllFilenames() {
		os.Remove(fn)
	}
	return nil
}

func (t *table) Index(name string, fieldNames []string) (items.IIndex, error) {
	if _, ok := t.index[name]; ok {
		return nil, fmt.Errorf("Duplicate db.Table(%s).Index(%s)", t.Name(), name)
	}

	newIndex, err := items.NewIndex(t, name, fieldNames)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to describe index")
	}

	//add the index to the table
	mi := &index{
		IIndex: newIndex,
		item:   make(map[string]items.IItem),
	}

	//if table is not empty, all items must be added to index now
	for _, itemFilename := range t.itemFilenames() {
		//load file...
		if item, err := t.readFile(itemFilename); err == nil {
			if err := mi.Add(item); err != nil {
				return nil, errors.Wrapf(err, "cannot add item to index")
			}
		} else {
			continue
		}
	}

	t.index[name] = mi
	return mi, nil
}

func (t *table) itemFilename(nid int, uid string, rev int) string {
	if rev > 0 {
		return fmt.Sprintf("%s/%s_%s_r%d.json", t.dir, t.Name(), uid, rev)
	}
	if rev < 0 {
		return fmt.Sprintf("%s/deleted_%s_%s.json", t.dir, t.Name(), uid)
	}
	return fmt.Sprintf("%s/%s_%s.json", t.dir, t.Name(), uid)
}

func (t *table) writeItem(i items.IItem) error {
	//wrap the item in an object to also capture the items.IItem values and the item.Data()
	itemData := make(map[string]interface{})
	itemData["uid"] = i.UID()
	itemData["nid"] = i.NID()
	itemData["revNr"] = i.Rev().Nr()
	itemData["revTs"] = i.Rev().Timestamp().Format(revTimestampFormat)
	itemData["data"] = i.Data()

	//encode it all as JSON
	jsonItemData, err := json.Marshal(itemData)
	if err != nil {
		return log.Wrapf(err, "Failed to encode item to JSON")
	}

	//determine item's filename
	//(without rev nr - this is current item data)
	//if this is an update, the current file must already be copied
	//to rev-nr copy before this is called, else old rev is overwritten
	itemFilename := t.itemFilename(i.NID(), i.UID(), 0)
	jsonFile, err := os.Create(itemFilename)
	if err != nil {
		return log.Wrapf(err, "Cannot create item file: %s", itemFilename)
	}
	defer jsonFile.Close()

	if _, err = jsonFile.Write(jsonItemData); err != nil {
		return log.Wrapf(err, "Failed to write into JSON file %s", itemFilename)
	}
	return nil
}

const revTimestampFormat = "20060102150405-0700"

//list of item files - only current version
func (t *table) itemFilenames() []string {
	list := []string{}
	filepath.Walk(t.dir, func(path string, info os.FileInfo, err error) error {
		if t.filenameRegex.MatchString(path) {
			list = append(list, path)
		}
		return nil
	})
	return list
}

//list of all item files, including older revisions and deleted items
func (t *table) itemAllFilenames() []string {
	list := []string{}
	filepath.Walk(t.dir, func(path string, info os.FileInfo, err error) error {
		if t.filenameAllRegex.MatchString(path) {
			list = append(list, path)
		}
		return nil
	})
	return list
}

func (t *table) readFile(fn string) (items.IItem, error) {
	jsonFile, err := os.Open(fn)
	if err != nil {
		return nil, log.Wrapf(err, "cannot open %s", fn)
	}
	defer jsonFile.Close()

	itemFileData := make(map[string]interface{})
	decoder := json.NewDecoder(jsonFile)
	decoder.UseNumber()
	if err := decoder.Decode(&itemFileData); err != nil {
		return nil, log.Wrapf(err, "failed to decode JSON file %s into bundle", fn)
	}

	nid, _ := strconv.Atoi(fmt.Sprintf("%v", itemFileData["nid"]))
	uid := itemFileData["uid"].(string)
	revNr, _ := strconv.Atoi(fmt.Sprintf("%v", itemFileData["revNr"]))
	revTs, err := time.Parse(revTimestampFormat, itemFileData["revTs"].(string))
	if err != nil {
		return nil, log.Wrapf(err, "Failed to parse JSON revTs=%v into time", itemFileData["revTs"])
	}
	jsonItemData, _ := json.Marshal(itemFileData["data"])

	itemDataPtrValue := reflect.New(t.Type())
	itemData := itemDataPtrValue.Interface().(items.IData)
	if err := json.Unmarshal(jsonItemData, itemData); err != nil {
		return nil, log.Wrapf(err, "failed to decode JSON item data: %v", err)
	}
	existingItem := items.NewItem(t,
		nid,
		uid,
		items.Rev(revNr, revTs),
		itemDataPtrValue.Elem().Interface().(items.IData))
	return existingItem, nil
}

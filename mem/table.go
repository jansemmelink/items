package mem

import (
	"fmt"
	"sync"
	"time"

	"github.com/jansemmelink/items"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type memTable struct {
	items.ITable
	mutex  sync.Mutex
	nextID int
	items  map[string]items.IItem
	index  map[string]items.IIndex
}

func (t *memTable) Count() int {
	if t == nil {
		return 0
	}
	return len(t.items)
}

func (t *memTable) AddItem(data items.IData) (items.IItem, error) {
	if t == nil {
		return nil, fmt.Errorf("nil.AddItem()")
	}
	if data == nil {
		return nil, fmt.Errorf("%s.AddItem(nil)", t.Name())
	}
	if err := data.Validate(); err != nil {
		return nil, errors.Wrapf(err, "invalid %v data", t.Type())
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	newItem := items.NewItem(t, t.nextID, uuid.NewV1().String(), items.Rev(1, time.Now()), data)
	for indexName, index := range t.index {
		if err := index.Add(newItem); err != nil {
			return nil, errors.Wrapf(err, "cannot add to index %s", indexName)
		}
	}

	t.items[newItem.UID()] = newItem
	t.nextID++
	return newItem, nil
}

func (t *memTable) UpdItem(upd items.IItem) (items.IItem, error) {
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

	t.mutex.Lock()
	defer t.mutex.Unlock()

	//get current revision of existing item
	cur, ok := t.items[upd.UID()]
	if !ok {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) not found", t.Name(), upd.NID(), upd.UID())
	}
	if cur.NID() != upd.NID() || cur.UID() != upd.UID() {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s) != CurItem(%d,%s)", t.Name(), upd.NID(), upd.UID(), cur.NID(), cur.UID())
	}

	//make sure this will be the next rev
	if upd.Rev().Nr() != cur.Rev().Nr()+1 {
		return nil, fmt.Errorf("%s.UpdItem(%d,%s).Rev.Nr=%d should be %d", t.Name(), upd.NID(), upd.UID(), upd.Rev().Nr(), cur.Rev().Nr()+1)
	}

	//correct: replace
	t.items[upd.UID()] = upd
	return upd, nil
}

func (t *memTable) GetItem(uid string) items.IItem {
	if t == nil {
		panic("nil.GetItem()")
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if existing, ok := t.items[uid]; ok {
		return existing
	}
	return nil
}

func (t *memTable) DelItem(old items.IItem) error {
	if t == nil {
		return fmt.Errorf("nil.DelItem()")
	}

	if old.Table() != t {
		return fmt.Errorf("%s.DelItem(nid=%d,uid=%s) from other table=%s", t.Name(), old.NID(), old.UID(), old.Table().Name())
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	//get current revision of existing item
	cur, ok := t.items[old.UID()]
	if !ok {
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
	delete(t.items, old.UID())
	return nil
}

func (t *memTable) Items() map[string]items.IItem {
	return t.items
}

func (t *memTable) DelAll() error {
	t.items = make(map[string]items.IItem)
	return nil
}

func (t *memTable) Index(name string, fieldNames []string) (items.IIndex, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if _, ok := t.index[name]; ok {
		return nil, fmt.Errorf("Duplicate db.Table(%s).Index(%s)", t.Name(), name)
	}

	newIndex, err := items.NewIndex(t, name, fieldNames)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to describe index")
	}

	//add the index to the table
	mi := &memIndex{
		IIndex: newIndex,
		item:   make(map[string]items.IItem),
	}

	//if table is not empty, all items must be added to index now
	for _, item := range t.items {
		err := mi.Add(item)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot add item to index")
		}
	}

	t.index[name] = mi
	return mi, nil
}

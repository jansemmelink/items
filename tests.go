package items

import (
	"fmt"

	"github.com/jansemmelink/log"
	"github.com/pkg/errors"
)

//RunDbTests ...
func RunDbTests(db IDb) error {
	log.DebugOn()
	log.Debugf("db=%T", db)

	if err := longTest(db); err != nil {
		return errors.Wrapf(err, "long test failed")
	}

	if err := twoFieldTest(db); err != nil {
		return errors.Wrapf(err, "twofield test failed")
	}

	return nil
}

func longTest(db IDb) error {
	users, err := db.Table("users", user{})
	if err != nil {
		return errors.Wrapf(err, "failed to add table")
	}
	log.Debugf("users=%T", users)

	if len(db.Tables()) != 1 {
		return fmt.Errorf("expected one table")
	}
	db.Table("sessions", session{})
	if len(db.Tables()) != 2 {
		return fmt.Errorf("expected two tables")
	}
	if _, err := db.Table("users", user{}); err == nil {
		return fmt.Errorf("managed to add dup table")
	}

	users.DelAll()

	uni, err := users.Index("username", []string{"Name"})
	if err != nil {
		return errors.Wrapf(err, "Failed to add username index")
	}

	u1, err := users.AddItem(user{Name: "one"})
	if err != nil {
		return errors.Wrapf(err, "Failed to add user one")
	}
	gotU1 := users.GetItem(u1.UID())
	if gotU1 == nil {
		return fmt.Errorf("Failed to get one")
	}
	if users.Count() != 1 {
		return fmt.Errorf("users.Count=%d", users.Count())
	}

	u2, err := users.AddItem(user{Name: "two"})
	if err != nil {
		return errors.Wrapf(err, "Failed to add user two")
	}
	gotU2 := users.GetItem(u2.UID())
	if gotU2 == nil {
		return fmt.Errorf("Failed to get two")
	}

	if gotU1.Rev().Nr() != 1 || gotU2.Rev().Nr() != 1 {
		return fmt.Errorf("Got %d %d", gotU1.Rev().Nr(), gotU2.Rev().Nr())
	}

	//find by index
	foundU1, err := uni.FindOne(map[string]interface{}{"Name": "one"})
	if err != nil {
		return errors.Wrapf(err, "failed to find u1 by username")
	}
	log.Debugf("Got U1: %v", foundU1)
	log.Debugf("Got foundU1: %+v", foundU1.Data())

	//update u1
	u1, err = u1.Upd(user{"ONE"})
	if err != nil {
		return errors.Wrapf(err, "Failed to rename u1")
	}
	if u1.Rev().Nr() != 2 {
		return fmt.Errorf("Rev=%d after update", u1.Rev().Nr())
	}

	gotU1x := users.GetItem(u1.UID())
	if gotU1x == nil {
		return fmt.Errorf("Failed to get ONE")
	}
	if gotU1x.NID() != u1.NID() || gotU1x.UID() != u1.UID() {
		return fmt.Errorf("Wrong ids %d!=%d or %s!=%s", gotU1x.NID(), u1.NID(), gotU1x.UID(), u1.UID())
	}
	if gotU1x.Rev().Nr() != 2 {
		return fmt.Errorf("Rev=%d != 2", gotU1x.Rev().Nr())
	}

	if users.Count() != 2 {
		return fmt.Errorf("users.Count=%d", users.Count())
	}

	//should fail to update from old copy
	_, err = gotU1.Upd(user{"ONEONE"})
	if err == nil {
		return fmt.Errorf("Should not be able to upd here")
	}
	log.Debugf("Nice failed to upd from old: %v", err)

	//should not be able to delete with old, but can del with new
	if err = gotU1.Del(); err == nil {
		return fmt.Errorf("Should not be able to del here")
	}
	if err = gotU1x.Del(); err != nil {
		return errors.Wrapf(err, "Failed to del")
	}
	if err = gotU2.Del(); err != nil {
		return errors.Wrapf(err, "Failed to del")
	}

	//after del, get should fail:
	gotU1y := users.GetItem(u1.UID())
	if gotU1y != nil {
		return fmt.Errorf("Got u1 after deletion")
	}
	log.Debugf("Good, failed to get u1 after delete")

	return nil
}

type user struct {
	Name string `sql:"name"`
}

func (u user) Validate() error {
	if len(u.Name) < 1 {
		return fmt.Errorf("missing user.name")
	}
	return nil
}

type session struct {
	sid   string
	uname string
}

func (s session) Validate() error {
	if len(s.sid) < 1 {
		return fmt.Errorf("missing session.sid")
	}
	if len(s.uname) < 1 {
		return fmt.Errorf("missing session.uname")
	}
	return nil
}

type person struct {
	Name    string
	Surname string
}

//Validate ...
func (p person) Validate() error {
	return nil
}

func twoFieldTest(db IDb) error {
	//create table with two string fields
	persons, err := db.Table("persons", person{})
	if err != nil {
		return errors.Wrapf(err, "failed to add table")
	}
	persons.DelAll()

	//both fields must be unique - put them in an index
	uni, err := persons.Index("unique", []string{"Name", "Surname"})
	if err != nil {
		return errors.Wrapf(err, "Failed to add index")
	}

	//add persons to the db
	list := []person{
		{Name: "a", Surname: "b"},
		{Name: "c", Surname: "d"},
		{Name: "e", Surname: "f"},
	}
	uidList := make([]string, 0)
	for _, pd := range list {
		pi, err := persons.AddItem(pd)
		if err != nil {
			return errors.Wrapf(err, "Failed to add person=%+v", pd)
		}
		uidList = append(uidList, pi.UID())
	}

	//retriev all by uid
	for i, uid := range uidList {
		pi := persons.GetItem(uid)
		if pi == nil {
			return fmt.Errorf("Failed to get on uid")
		}
		pd := pi.Data().(person)
		if pd.Name != list[i].Name || pd.Surname != list[i].Surname {
			return fmt.Errorf("got [%d]uid=%s.%+v != %+v", i, uid, pd, list[i])
		}
	}

	//retrieve by name and surname
	for i, p := range list {
		pi, err := uni.FindOne(map[string]interface{}{"Name": p.Name, "Surname": p.Surname})
		if err != nil {
			return errors.Wrapf(err, "failed to find by name and surname %+v", p)
		}
		pd := pi.Data().(person)
		if pd.Name != p.Name || pd.Surname != p.Surname || pi.UID() != uidList[i] {
			return fmt.Errorf("got [%d]uid=%s.%+v != %s.%+v", i, pi.UID(), pd, uidList[i], p)
		}
	}

	// //update u1
	// u1, err = u1.Upd(user{"ONE"})
	// if err != nil {
	// 	return errors.Wrapf(err, "Failed to rename u1")
	// }
	// if u1.Rev().Nr() != 2 {
	// 	return fmt.Errorf("Rev=%d after update", u1.Rev().Nr())
	// }

	// gotU1x := users.GetItem(u1.UID())
	// if gotU1x == nil {
	// 	return fmt.Errorf("Failed to get ONE")
	// }
	// if gotU1x.NID() != u1.NID() || gotU1x.UID() != u1.UID() {
	// 	return fmt.Errorf("Wrong ids %d!=%d or %s!=%s", gotU1x.NID(), u1.NID(), gotU1x.UID(), u1.UID())
	// }
	// if gotU1x.Rev().Nr() != 2 {
	// 	return fmt.Errorf("Rev=%d != 2", gotU1x.Rev().Nr())
	// }

	// if users.Count() != 2 {
	// 	return fmt.Errorf("users.Count=%d", users.Count())
	// }

	// //should fail to update from old copy
	// _, err = gotU1.Upd(user{"ONEONE"})
	// if err == nil {
	// 	return fmt.Errorf("Should not be able to upd here")
	// }
	// log.Debugf("Nice failed to upd from old: %v", err)

	// //should not be able to delete with old, but can del with new
	// if err = gotU1.Del(); err == nil {
	// 	return fmt.Errorf("Should not be able to del here")
	// }
	// if err = gotU1x.Del(); err != nil {
	// 	return errors.Wrapf(err, "Failed to del")
	// }
	// if err = gotU2.Del(); err != nil {
	// 	return errors.Wrapf(err, "Failed to del")
	// }

	// //after del, get should fail:
	// gotU1y := users.GetItem(u1.UID())
	// if gotU1y != nil {
	// 	return fmt.Errorf("Got u1 after deletion")
	// }
	// log.Debugf("Good, failed to get u1 after delete")

	return nil
} //twoFieldTest()

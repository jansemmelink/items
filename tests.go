package items

import (
	"fmt"

	"github.com/jansemmelink/log"
	"github.com/pkg/errors"
)

//RunDbTests ...
func RunDbTests(db IDb) error {
	log.DebugOn()
	users, err := db.AddTable(Table("users", user{}))
	if err != nil {
		return errors.Wrapf(err, "failed to add table")
	}

	if len(db.Tables()) != 1 {
		return fmt.Errorf("expected one table")
	}
	db.AddTable(Table("sessions", session{}))
	if len(db.Tables()) != 2 {
		return fmt.Errorf("expected two tables")
	}
	if _, err := db.AddTable(Table("users", user{})); err == nil {
		return fmt.Errorf("managed to add dup table")
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

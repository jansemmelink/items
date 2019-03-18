package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/jansemmelink/items"
	"github.com/jansemmelink/log"
	jsql "github.com/jansemmelink/sql"
	"github.com/pkg/errors"
)

//New creates a new SQL database with the specified connection configuration
func New(c jsql.Connection) (items.IDb, error) {
	if err := c.Validate(); err != nil {
		return nil, errors.Wrapf(err, "invalid sql config")
	}
	sqlConn, err := c.Connect()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect")
	}

	log.Debugf("Connected to %+v", c)
	return &sqlDatabase{
		IDb:  items.New(c.Database),
		conn: sqlConn,
	}, nil
}

//sqlDatabase extends the default items.Database to store in SQL
type sqlDatabase struct {
	items.IDb
	conn *sql.DB
}

func (db *sqlDatabase) Table(name string, tmplStruct items.IData) (items.ITable, error) {
	//we get here to add the table to SQL before it is accepted into the items.IDb that we embed
	log.Debugf("sqlDatabase.AddTable(conn=%v)", db.conn)

	//see if can add to the db, but delete if not able to add to SQL
	t, err := db.IDb.Table(name, tmplStruct)
	if err != nil {
		return nil, errors.Wrapf(err, "db(%s).table(%s) failed.", db.Name(), name)
	}

	defer func() {
		if t != nil {
			db.RemTable(t)
		}
	}()

	//create a new SQL table or validate the structure of an existing table
	tableName := "tbl_" + name
	existingTableFields, err := jsql.Describe(db.conn, tableName)
	if err == nil {
		log.Debugf("Table %s exists with %d fields:", tableName, len(existingTableFields))
		//table exists
		//todo: compare with what we expect
		for i, tfd := range existingTableFields {
			log.Errorf("   TODO compare existing SQL table field[%d]: %+v", i, tfd)
		}
	} else {
		//table does not exist, create
		log.Debugf("Creating table %s ...:", tableName)
		fieldDefs, err := structFieldDefs(t.Type())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to describe %s as SQL table fields", tableName)
		}

		//if the table does not exist, it must be created, or
		//the table must exist with the correct definition
		sqlQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (", tableName)
		//header fields
		sqlQuery += " nid int AUTO_INCREMENT PRIMARY KEY"
		sqlQuery += ",uid char(40) NOT NULL"
		sqlQuery += ",revNr int NOT NULL"
		sqlQuery += ",revTs char(18) NOT NULL" //ts format: "CCYYMMDDHHMMSS.000" in UTC always
		//user data fields from reflectType of user data struct
		sqlQuery += "," + fieldDefs
		//indexes and keys
		sqlQuery += ",INDEX `idx_%s_uid` (uid)"
		sqlQuery += ",UNIQUE KEY (uid,revNr)"
		//end of table definition
		sqlQuery += ") ENGINE=InnoDB DEFAULT CHARSET=utf8"

		rows, err := db.conn.Query(sqlQuery)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create table %s: %s", tableName, sqlQuery)
		}
		for rows.Next() {
			cols, _ := rows.Columns()
			log.Debugf("Got a row: %+v", cols)
		}
	}

	//SQL happy, call the embedded method to make it part of the database
	//and wrap the table in an sqlTable so we will be called for all table operations
	log.Debugf("SQL Table ok. Adding to db...")
	st := &sqlTable{
		ITable:        t,
		conn:          db.conn,
		tableName:     tableName,
		csvFieldNames: items.StructFields(t.Type()),
	}
	t = nil
	return st, nil
}

func structFieldDefs(structType reflect.Type) (string, error) {
	fieldDef := ""
	for fieldIndex := 0; fieldIndex < structType.NumField(); fieldIndex++ {
		//fieldValue := v.Field(fieldIndex)
		structField := structType.Field(fieldIndex)
		log.Debugf("Field[%d]: %+v", fieldIndex, structField.Name)
		// structField.CanInterface()

		sqlType := ""
		sqlOptions := ""
		switch structField.Type.Kind() {
		case reflect.String:
			sqlType = "varchar(255)"
			sqlOptions = "NOT NULL"
		case reflect.Int:
			sqlType = "int"
			sqlOptions = "NOT NULL"
		case reflect.Float32:
			sqlType = "decimal(5,2)"
			sqlOptions = "NOT NULL"
		case reflect.Struct:
			switch structField.Type {
			case reflect.TypeOf(time.Time{}):
				sqlType = "datetime"
				sqlOptions = "NOT NULL"
			default:
				return "", fmt.Errorf("no SQL definition for %s.%s of %v %v", structType.Name(), structField.Name, structField.Type.Kind(), structField.Type.Name())
			}
		default:
			return "", fmt.Errorf("no SQL definition for %s.%s of kind %v", structType.Name(), structField.Name, structField.Type.Kind())
		}

		fieldDef += fmt.Sprintf(",%s %s %s", structField.Name, sqlType, sqlOptions)
	}
	if len(fieldDef) < 1 {
		log.Debugf("%v sql def: \"\"", structType.Name())
		return "", nil
	}

	log.Debugf("%v sql def: %s", structType.Name(), fieldDef[1:])
	return fieldDef[1:], nil
}

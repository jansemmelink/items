package items

import (
	"fmt"
	"reflect"
)

//ISchema is create from IData to iterate over fields and sub-structures
type ISchema interface{}

//NewSchema from a reflect structure type
func NewSchema(t reflect.Type) (ISchema, error) {
	return schema{t}, nil
}

type schema struct {
	t reflect.Type
}

//StructFields list the exported fields of the struct in CSV e.g. "Name,Surname"
func StructFields(t reflect.Type) string {
	//dereference to struct level
	t = structType(t)
	p := reflect.New(t).Interface()
	v := reflect.ValueOf(p).Elem()

	//log.Debugf("StructField(%v) -> t=%v p=%T v=%v", t, t, p, v)
	csvFieldNames := ""
	for fieldIndex := 0; fieldIndex < t.NumField(); fieldIndex++ {
		fieldValue := v.Field(fieldIndex)
		if fieldValue.CanSet() {
			fieldType := t.Field(fieldIndex)
			csvFieldNames += "," + fieldType.Name
		}
	}

	if len(csvFieldNames) < 1 {
		return ""
	}
	return csvFieldNames[1:]
}

func structType(t reflect.Type) reflect.Type {
	t1 := t
	if t1.Kind() == reflect.Ptr {
		t1 = t1.Elem()
	}
	if t1.Kind() != reflect.Struct {
		panic(fmt.Sprintf("%v is not a struct", t))
	}
	return t1
}

package items

import "fmt"

//IKey ...
type IKey interface {
	With(n string, v interface{}) IKey
	String() string
}

//key implements IKey
type key struct {
	name  []string
	value []interface{}
}

//NewKey creates a key
func NewKey() IKey {
	return key{
		name:  make([]string, 0),
		value: make([]interface{}, 0),
	}
}

func (k key) With(n string, v interface{}) IKey {
	k.name = append(k.name, n)
	k.value = append(k.value, v)
	return k
}

func (k key) String() string {
	s := ""
	for _, v := range k.value {
		s += "," + fmt.Sprintf("%v", v)
	}
	if len(s) < 1 {
		return ""
	}
	return s[1:]
}

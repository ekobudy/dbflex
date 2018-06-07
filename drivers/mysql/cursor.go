package mysql

import (
	"reflect"
	"strconv"

	"github.com/eaciit/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

func (c *Cursor) SerializeFieldType(name string, dtype reflect.Type, value interface{}) (interface{}, error) {
	dtypestr := dtype.(reflect.Type).String()
	switch dtypestr {
	case "time.Time":
		return toolkit.ToDate(value.(string), "yyyy-MM-dd hh:mm:ss"), nil
	case "int", "int32", "int64":
		v, e := strconv.Atoi(toolkit.ToString(value))
		if e != nil {
			return int(0), toolkit.Errorf("%s=%v can't be serialised to int", name, value)
		}
		return v, nil
	case "float", "float32", "float64":
		val, e := strconv.ParseFloat(toolkit.ToString(value), 64)
		if e != nil {
			return float64(0), toolkit.Errorf("%s=%v can't be serialised to float", name, value)
		} else {
			return val, nil
		}
	case "bool":
		valstr := toolkit.ToString(value)
		return (valstr == "1" || valstr == "true"), nil
	default:
		return toolkit.ToString(value), nil
	}
}

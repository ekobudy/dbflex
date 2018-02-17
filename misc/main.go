package main

import (
	"reflect"
	"strconv"
	"time"

	"github.com/eaciit/toolkit"
)

type MachineTypeEnum string
type MachineRateEnum int

const (
	General MachineTypeEnum = "General"
	Boiler                  = "Boiler"
	Special                 = "Special"
)

const (
	Low MachineRateEnum = 1 + iota
	Medium
	High
)

type Machine struct {
	ID          string
	Name        string
	Serial      string
	MachineType MachineTypeEnum
	MachineRate MachineRateEnum
	Age         int
	Capacity    float64
	Purchased   time.Time
	Active      bool
}

func main() {
	m := &Machine{"M01", "General Machine 01", "M001", General, Medium, 13, 45.32, time.Now(), true}

	//for i := 0; i < 10; i++ {
	fields, types, values, sqlvalues := deepTrace(m)
	toolkit.Printf("Fields: %s \n"+
		"Types: %s\n"+
		"Values: %s\n"+
		"Sql: %s\n", toolkit.JsonString(fields), toolkit.JsonString(types), toolkit.JsonString(values), toolkit.JsonString(sqlvalues))

	fields, types, values, sqlvalues = deepTrace(toolkit.M{}.
		Set("ID", 2010).
		Set("Name", "Emp 2010").
		Set("BirthDate", toolkit.String2Date("01-04-1980", "dd-MM-yyyy")))
	toolkit.Printf("Fields: %s \n"+
		"Types: %s\n"+
		"Values: %s\n"+
		"Sql: %s\n", toolkit.JsonString(fields), toolkit.JsonString(types), toolkit.JsonString(values), toolkit.JsonString(sqlvalues))

	fields, types, values, sqlvalues = deepTrace(float64(367.78))
	toolkit.Printf("Fields: %s \n"+
		"Types: %s\n"+
		"Values: %s\n"+
		"Sql: %s\n", toolkit.JsonString(fields), toolkit.JsonString(types), toolkit.JsonString(values), toolkit.JsonString(sqlvalues))
	//}
}

//deepTrace returns names, types, values and sql value as string
func deepTrace(o interface{}) ([]string, []reflect.Type, []interface{}, []string) {
	r := reflect.Indirect(reflect.ValueOf(o))
	t := r.Type()
	names := []string{}
	types := []reflect.Type{}
	values := []interface{}{}
	sqlnames := []string{}

	if r.Kind() == reflect.Struct {
		nf := r.NumField()
		for fieldIdx := 0; fieldIdx < nf; fieldIdx++ {
			f := r.Field(fieldIdx)
			ft := t.Field(fieldIdx)
			v := f.Interface()
			names = append(names, ft.Name)
			types = append(types, ft.Type)
			values = append(values, v)
			sqlnames = append(sqlnames, sqlFormat(v))
		}
	} else if r.Kind() == reflect.Map {
		keys := r.MapKeys()
		for _, k := range keys {
			names = append(names, toolkit.Sprintf("%v", k.Interface()))
			types = append(types, k.Type())

			value := r.MapIndex(k)
			v := value.Interface()
			values = append(values, v)
			sqlnames = append(sqlnames, sqlFormat(v))
		}
	} else {
		names = append(names, t.Name())
		types = append(types, t)
		values = append(values, o)
		sqlnames = append(sqlnames, sqlFormat(o))
	}

	return names, types, values, sqlnames
}

// valueTrace returns sqlvalues
func sqlFormat(v interface{}) string {
	if s, ok := v.(string); ok {
		return toolkit.Sprintf("'%s'", s)
	} else if _, ok := v.(int); ok {
		return toolkit.Sprintf("%d", v)
	} else if _, ok = v.(float64); ok {
		return toolkit.Sprintf("%f", v)
	} else if _, ok = v.(time.Time); ok {
		return toolkit.Date2String(v.(time.Time), "yyyy-MM-dd hh:mm:ss")
	} else if b, ok := v.(bool); ok {
		if b {
			return "1"
		} else {
			return "0"
		}
	} else {
		vstr := toolkit.Sprintf("%v", v)
		if _, err := strconv.ParseFloat(vstr, 64); err == nil {
			return vstr
		} else {
			return "'" + vstr + "'"
		}
	}
}

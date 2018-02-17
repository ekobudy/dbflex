package rdbms

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/eaciit/toolkit"

	"github.com/eaciit/dbflex"
)

type IRdbmsCursor interface {
	Serialize(interface{}) error
	SerializeField(string, interface{}) (interface{}, error)
}

type Cursor struct {
	dbflex.CursorBase
	fetcher   *sql.Rows
	dest      []interface{}
	columns   []string
	values    []interface{}
	valuesPtr []interface{}
	m         toolkit.M

	_this        dbflex.ICursor
	dataTypeList toolkit.M
}

func (c *Cursor) Reset() error {
	c.fetcher = nil
	c.dest = []interface{}{}
	return nil
}

func (c *Cursor) SetFetcher(r *sql.Rows) error {
	c.fetcher = r
	c.m = toolkit.M{}

	var err error
	c.columns, err = c.fetcher.Columns()
	if err != nil {
		return fmt.Errorf("unable to fetch columns. %s", err.Error())
	}

	count := len(c.columns)
	c.values = make([]interface{}, count)
	c.valuesPtr = make([]interface{}, count)

	for i, v := range c.columns {
		c.valuesPtr[i] = &c.values[i]
		c.m.Set(v, i)
	}
	return nil
}

func (c *Cursor) SetThis(ic dbflex.ICursor) dbflex.ICursor {
	c._this = ic
	return c
}

func (c *Cursor) this() dbflex.ICursor {
	return c._this
}

func (c *Cursor) Scan() error {
	if c.Error() != nil {
		return c.Error()
	}

	if c.fetcher == nil {
		return toolkit.Error("cursor is not valid, no fetcher object specified")
	}

	if !c.fetcher.Next() {
		return toolkit.Error("EOF")
	}

	return c.fetcher.Scan(c.valuesPtr...)
}

func (c *Cursor) SerializeField(name string, value interface{}) (interface{}, error) {
	for k, dtype := range c.dataTypeList {
		if strings.ToLower(k) == strings.ToLower(name) {
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
	}

	return nil, toolkit.Errorf("field or attribute %s could not be found", name)
}

func (c *Cursor) Serialize(dest interface{}) error {
	var err error
	mobj := toolkit.M{}
	toolkit.Serde(c.m, &mobj, "")

	//-- if dateTypeList if not yet created, create new one
	if len(c.dataTypeList) == 0 {
		for k, v := range c.m {
			c.dataTypeList.Set(k, reflect.TypeOf(c.valuesPtr[v.(int)]))
		}
	}

	for k, v := range c.m {
		var vtr interface{}
		if vtr, err = c.this().(IRdbmsCursor).SerializeField(k, string(c.values[v.(int)].([]byte))); err != nil {
			return err
		} else {
			mobj.Set(k, vtr)
		}
	}
	return toolkit.Serde(mobj, dest, "")
}

func (c *Cursor) GetDataTypeString(name string) string {
	if c.dataTypeList == nil {
		c.dataTypeList = toolkit.M{}
	}
	t := c.dataTypeList.Get(name, nil)
	if t == nil {
		return ""
	} else {
		return t.(reflect.Type).String()
	}
}

func (c *Cursor) Fetch(obj interface{}) error {
	err := c.Scan()
	if err != nil {
		return err
	}
	c.GetTypeList(obj)
	err = c.this().(IRdbmsCursor).Serialize(obj)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cursor) Fetchs(obj interface{}, n int) error {
	var err error

	//--- get first model
	if c.dataTypeList != nil {
		var single interface{}
		if single, err = toolkit.GetEmptySliceElement(obj); err != nil {
			return toolkit.Errorf("unable to get slice elemnt: %s", err.Error())
		}
		c.GetTypeList(single)
	}

	i := 0
	loop := true
	ms := []toolkit.M{}
	for loop {
		err = c.Scan()
		if err != nil {
			if n == 0 {
				loop = false
			} else {
				return err
			}
		} else {
			mobj := toolkit.M{}
			err = c.this().(IRdbmsCursor).Serialize(&mobj)
			if err != nil {
				return err
			}
			ms = append(ms, mobj)
			i++
			if i == n {
				loop = false
			}
		}
	}

	//err = toolkit.Serde(ms, obj, "")
	if err != nil {
		return err
	}
	return nil
}

func (c *Cursor) Count() int {
	return 0
}

func (c *Cursor) Close() {
	if c.fetcher != nil {
		c.fetcher.Close()
	}
}

func (c *Cursor) GetTypeList(obj interface{}) {
	c.dataTypeList = toolkit.M{}
	fieldnames, fieldtypes, _, _ := ParseSQLMetadata(obj)
	for i, name := range fieldnames {
		c.dataTypeList.Set(name, fieldtypes[i])
	}
}

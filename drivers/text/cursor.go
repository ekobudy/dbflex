package text

import (
	"bufio"
	"os"
	"reflect"

	"github.com/eaciit/toolkit"

	"github.com/eaciit/dbflex"
)

type Cursor struct {
	dbflex.CursorBase

	f       *os.File
	scanner *bufio.Scanner
}

func (c *Cursor) Reset() error {
	panic("not implemented")
}

func (c *Cursor) Fetch(out interface{}) error {
	eof := c.scanner.Scan()
	if !eof {
		data := c.scanner.Text()
		return textToObj(data, out, cfg)
	}
	return nil
}

func (c *Cursor) Fetchs(result interface{}, n int) error {
	loop := true
	read := 0
	v := reflect.TypeOf(result).Elem().Elem()
	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	for c.scanner.Scan() && loop {
		read++
		data := c.scanner.Text()
		iv := reflect.New(v).Interface()
		err := textToObj(data, iv, cfg)
		if err != nil {
			return toolkit.Errorf("unable to serialize data. %s - %s", data, err.Error())
		}
		ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
		if read == n {
			loop = false
		}
	}
	reflect.ValueOf(result).Elem().Set(ivs)

	return nil
}

func (c *Cursor) Count() int {
	return 0
}

func (c *Cursor) Close() {
	if c.f != nil {
		c.f.Close()

		c.f = nil
		c.scanner = nil
	}
}

func (c *Cursor) openFile(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(f)

	c.f = f
	c.scanner = scanner
	return nil
}

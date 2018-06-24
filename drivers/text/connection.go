package text

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/eaciit/toolkit"

	"github.com/eaciit/dbflex"
)

func init() {
	//=== sample: text://localhost?path=/usr/local/txt
	dbflex.RegisterDriver("text", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.ServerInfo = *si
		c.SetThis(c)
		c.SetFieldNameTag("sql")
		return c
	})
}

type Connection struct {
	dbflex.ConnectionBase

	dirInfo        os.FileInfo
	dirPath        string
	extension      string
	textObjSetting *TextObjSetting
}

func (c *Connection) Connect() error {
	//panic("not implemented")

	dirpath := c.Database
	if dirpath == "" {
		return toolkit.Errorf("")
	}

	fi, err := os.Stat(dirpath)
	if err != nil {
		return err
	}

	if fi.IsDir() == false {
		return toolkit.Errorf("%s is not a directory", dirpath)
	}

	c.dirInfo = fi
	c.dirPath = dirpath

	c.extension = c.Config.Get("extension", "").(string)
	c.textObjSetting = c.Config.Get("text_obj_setting", NewTextObjSetting(',')).(*TextObjSetting)
	return nil
}

func (c *Connection) State() string {
	//panic("not implemented")
	if c.dirInfo != nil {
		return dbflex.StateConnected
	} else {
		return dbflex.StateUnknown
	}
}

func (c *Connection) Close() {
	c.dirInfo = nil
	c.dirPath = ""
}

func (c *Connection) NewQuery() dbflex.IQuery {
	//panic("not implemented")
	q := new(Query)
	q.SetThis(q)
	q.SetConnection(c)
	q.textObjectSetting = c.textObjSetting
	return q
}

func (c *Connection) ObjectNames(dbflex.ObjTypeEnum) []string {
	files, err := ioutil.ReadDir(c.dirPath)
	if err != nil {
		return []string{}
	}

	names := []string{}
	for _, fi := range files {
		name := strings.ToLower(fi.Name())
		if len(c.extension) == 0 {
			names = append(names, name)
		} else {
			if strings.HasSuffix(name, "."+c.extension) {
				names = append(names, name[0:len(name)-len(c.extension)-1])
			}
		}
	}
	return names
}

func (c *Connection) ValidateTable(interface{}, bool) error {
	return nil
}

func (c *Connection) DropTable(name string) error {
	filepath := filepath.Join(c.dirPath, name)
	return os.Remove(filepath)
}

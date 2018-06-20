package text

import (
	"os"
	"path/filepath"

	"github.com/eaciit/dbflex"
	"github.com/eaciit/toolkit"
)

type Query struct {
	dbflex.QueryBase
}

func (q *Query) BuildFilter(*dbflex.Filter) (interface{}, error) {
	return nil, nil
}

func (q *Query) BuildCommand() (interface{}, error) {
	return nil, nil
}

func (q *Query) Cursor(toolkit.M) dbflex.ICursor {
	c := new(Cursor)
	c.SetThis(c)
	c.SetConnection(q.Connection())
	return c
}

func (q *Query) Execute(toolkit.M) (interface{}, error) {
	conn := q.Connection().(*Connection)
	filename := ""
	cmdType := q.Config(dbflex.ConfigKeyCommandType, "").(string)
	tablename := q.Config(dbflex.ConfigKeyTableName, "").(string)

	if tablename == "" {
		return nil, toolkit.Errorf("no tablename is specified")
	} else {
		filename = tablename + "." + conn.extension
	}
	filePath := filepath.Join(conn.dirPath, filename)

	fileExist := false
	if _, err := os.Stat(filePath); err == nil {
		fileExist = true
	}

	//-- if save, insert and update. create the file
	if (cmdType == dbflex.QueryInsert ||
		cmdType == dbflex.QuerySave ||
		cmdType == dbflex.QueryUpdate ||
		cmdType == dbflex.QueryDelete) && !fileExist {
		_, err := os.Create(filePath)
		if err != nil {
			return err, toolkit.Errorf("unable to create file %s. %s", filePath, err.Error())
		}
	}

	switch cmdType {
	case dbflex.QuerySelect:
		return nil, toolkit.Errorf("select command should use cursor instead of execute")

	case dbflex.QueryInsert:

	case dbflex.QueryDelete:
		if !fileExist {
			return nil, nil
		}

	default:
		return nil, toolkit.Errorf("unknown command: %s", cmdType)
	}

	return nil, nil
}

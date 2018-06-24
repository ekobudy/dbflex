package text

import (
	"bufio"
	"os"
	"path/filepath"

	"github.com/eaciit/dbflex"
	"github.com/eaciit/toolkit"
)

type Query struct {
	dbflex.QueryBase

	textObjectSetting *TextObjSetting
}

func (q *Query) BuildFilter(*dbflex.Filter) (interface{}, error) {
	return nil, nil
}

func (q *Query) BuildCommand() (interface{}, error) {
	return nil, nil
}

func (q *Query) filePath() (string, error) {
	conn := q.Connection().(*Connection)
	filename := ""
	tablename := q.Config(dbflex.ConfigKeyTableName, "").(string)

	if tablename == "" {
		return "", toolkit.Errorf("no tablename is specified")
	} else {
		filename = tablename + "." + conn.extension
	}
	filePath := filepath.Join(conn.dirPath, filename)
	return filePath, nil
}

func (q *Query) Cursor(toolkit.M) dbflex.ICursor {
	c := new(Cursor)
	c.SetThis(c)
	c.SetConnection(q.Connection())

	filePath, err := q.filePath()
	if err != nil {
		c.SetError(err)
	}
	if _, err := os.Stat(filePath); err != nil {
		if err == os.ErrNotExist {
			//-- do something here
		} else {
			c.SetError(err)
			return c
		}
	}

	c.filePath = filePath
	c.openFile()
	c.textObjectSetting = q.textObjectSetting
	return c
}

func (q *Query) Execute(parm toolkit.M) (interface{}, error) {
	cfg := q.textObjectSetting
	cmdType := q.Config(dbflex.ConfigKeyCommandType, "").(string)
	filePath, err := q.filePath()
	if err != nil {
		return nil, err
	}

	fileExist := false
	if _, err = os.Stat(filePath); err == nil {
		fileExist = true
	}

	var file *os.File
	//-- if save, insert and update. create the file
	if (cmdType == dbflex.QueryInsert ||
		cmdType == dbflex.QuerySave ||
		cmdType == dbflex.QueryUpdate ||
		cmdType == dbflex.QueryDelete) && !fileExist {
		file, err = os.Create(filePath)
		if err != nil {
			return err, toolkit.Errorf("unable to create file %s. %s", filePath, err.Error())
		}
	} else {
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, os.ModeAppend)
		if err != nil {
			return err, toolkit.Errorf("unable to open file %s. %s", filePath, err.Error())
		}
	}

	defer func() {
		file.Close()
	}()

	switch cmdType {
	case dbflex.QuerySelect:
		return nil, toolkit.Errorf("select command should use cursor instead of execute")

	case dbflex.QueryInsert:
		data, hasData := parm["data"]
		if !hasData {
			return nil, toolkit.Errorf("insert fail, no data")
		}
		txt, err := objToText(data, cfg)
		if err != nil {
			return nil, toolkit.Errorf("error serializing data into text. %s", err.Error())
		}

		txt = "something to write"
		_, err = file.WriteString(txt + "\n")
		if err != nil {
			return nil, toolkit.Errorf("unable to write to text file %s. %s", filePath, err.Error())
		}
		err = file.Sync()
		if err != nil {
			return nil, toolkit.Errorf("unable to write to text file %s. %s", filePath, err.Error())
		}
	case dbflex.QueryDelete:
		if !fileExist {
			return nil, nil
		}

		deleteAll := false
		if deleteAll {
			stat, _ := file.Stat()
			err = file.Truncate(stat.Size())
			if err != nil {
				return nil, toolkit.Errorf("unable to truncated %s", err.Error())
			}
		} else {
			var tempFile *os.File
			tempFileName := filePath + "_temp_" + toolkit.RandomString(32)
			if tempFile, err = os.Create(tempFileName); err != nil {
				return nil, toolkit.Errorf("unable to create temp file. %s", err.Error())
			}
			defer tempFile.Close()

			reader := bufio.NewScanner(file)
			for reader.Scan() {
				txt := reader.Text()
				tempFile.WriteString(txt + "\n")
			}
			tempFile.Sync()
		}

	default:
		return nil, toolkit.Errorf("unknown command: %s", cmdType)
	}

	return nil, nil
}

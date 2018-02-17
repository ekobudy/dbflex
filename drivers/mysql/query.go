package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/eaciit/dbflex"
	"github.com/eaciit/dbflex/drivers/rdbms"
	"github.com/eaciit/toolkit"
)

type Query struct {
	rdbms.Query
	db         *sql.DB
	sqlcommand string
}

func (q *Query) Cursor(in toolkit.M) dbflex.ICursor {
	cursor := new(Cursor)
	cursor.SetThis(cursor)

	if err := q.Prepare(); err != nil {
		cursor.SetError(toolkit.Errorf("prepare: %s", err.Error()))
		return cursor
	}

	if q.CommandType() != dbflex.QuerySelect && q.CommandType() != dbflex.QuerySQL {
		cursor.SetError(toolkit.Errorf("cursor is used for only select command"))
		return cursor
	}

	cmdtxt := q.GetConfig(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" {
		cursor.SetError(toolkit.Errorf("no command"))
		return cursor
	}

	fmt.Println("Sql cursor command", cmdtxt)
	rows, err := q.db.Query(cmdtxt)
	if rows == nil {
		cursor.SetError(toolkit.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt))
	} else {
		cursor.SetFetcher(rows)
	}
	return cursor
}

func (q *Query) Execute(in toolkit.M) (interface{}, error) {
	if err := q.Prepare(); err != nil {
		return nil, toolkit.Errorf("prepare: %s", err.Error())
	}

	cmdtype := q.CommandType()
	cmdtxt := q.GetConfig(dbflex.ConfigKeyCommand, "").(string)
	if cmdtxt == "" {
		return nil, toolkit.Errorf("No command")
	}

	data, hasData := in["data"]
	if !hasData && !(cmdtype == dbflex.QueryDelete || cmdtype == dbflex.QuerySelect) {
		return nil, toolkit.Error("non select and delete command should has data")
	}
	sqlfieldnames, _, _, sqlvalues := rdbms.ParseSQLMetadata(data)
	affectedfields := q.GetConfig("fields", []string{}).([]string)
	if len(affectedfields) > 0 {
		newfieldnames := []string{}
		newvalues := []string{}
		for idx, field := range sqlfieldnames {
			for _, find := range affectedfields {
				if strings.ToLower(field) == strings.ToLower(find) {
					newfieldnames = append(newfieldnames, find)
					newvalues = append(newvalues, sqlvalues[idx])
				}
			}
		}
		sqlfieldnames = newfieldnames
		sqlvalues = newvalues
	}

	switch cmdtype {
	case dbflex.QueryInsert:
		cmdtxt = strings.Replace(cmdtxt, "{{.FIELDS}}", strings.Join(sqlfieldnames, ","), -1)
		cmdtxt = strings.Replace(cmdtxt, "{{.VALUES}}", strings.Join(sqlvalues, ","), -1)

	case dbflex.QueryUpdate:
		//fmt.Println("fieldnames:", sqlfieldnames)
		updatedfields := []string{}
		for idx, fieldname := range sqlfieldnames {
			updatedfields = append(updatedfields, fieldname+"="+sqlvalues[idx])
		}
		cmdtxt = strings.Replace(cmdtxt, "{{.FIELDVALUES}}", strings.Join(updatedfields, ","), -1)
	}

	//fmt.Println("Cmd: ", cmdtxt)
	r, err := q.db.Exec(cmdtxt)

	if err != nil {
		return nil, toolkit.Errorf("%s. SQL Command: %s", err.Error(), cmdtxt)
	}
	return r, nil
}

type ExecType int

const (
	ExecQuery ExecType = iota
	ExecNonQuery
	ExecQueryRow
)

/*
func (q *Query) SQL(string cmd, exec) dbflex.IQuery {
	swicth()
}
*/

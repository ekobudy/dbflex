package dbflex

import (
	"fmt"

	"github.com/eaciit/toolkit"
)

const (
	ConfigKeyCommand           string = "dbfcmd"
	ConfigKeyCommandType              = "dbfcmdtype"
	ConfigKeyGroupedQueryItems        = "dbfgqis"
	ConfigKeyWhere                    = "dbfwhere"
	ConfigKeyTableName                = "tablenames"
	ConfigKeyFilter                   = "filter"
)

type IQuery interface {
	This() IQuery
	BuildFilter(*Filter) (interface{}, error)
	BuildCommand() (interface{}, error)

	Cursor(toolkit.M) ICursor
	Execute(toolkit.M) (interface{}, error)

	SetConfig(string, interface{})
	SetConfigM(toolkit.M)
	Config(string, interface{}) interface{}
	ConfigRef(string, interface{}, interface{})
	DeleteConfig(...string)

	Connection() IConnection
	SetConnection(IConnection)
}

type QueryBase struct {
	items []*QueryItem

	self        IQuery
	commandType string

	prepared bool
	cmd      ICommand
	conn     IConnection

	config toolkit.M
}

type GroupedQueryItems map[string][]*QueryItem

func (q *QueryBase) initConfig() {
	if q.config == nil {
		q.config = toolkit.M{}
	}
}

func (q *QueryBase) Connection() IConnection {
	return q.conn
}

func (q *QueryBase) SetConnection(conn IConnection) {
	q.conn = conn
}

func (q *QueryBase) SetConfig(key string, value interface{}) {
	q.initConfig()
	q.config.Set(key, value)
}

func (q *QueryBase) SetConfigM(in toolkit.M) {
	for k, v := range in {
		q.SetConfig(k, v)
	}
}

func (q *QueryBase) Config(key string, def interface{}) interface{} {
	q.initConfig()
	return q.config.Get(key, def)
}

func (q *QueryBase) ConfigRef(key string, def, out interface{}) {
	q.initConfig()
	q.config.Get(key, def, out)
}

func (q *QueryBase) DeleteConfig(deletedkeys ...string) {
	q.initConfig()
	for _, delkey := range deletedkeys {
		delete(q.config, delkey)
	}
}

func (b *QueryBase) BuildCommand() (interface{}, error) {
	return nil, fmt.Errorf("Parse command is not yet implemented")
}

func buildGroupedQueryItems(cmd ICommand, b IQuery) error {
	groupeditems := GroupedQueryItems{}
	for _, i := range cmd.(*CommandBase).items {
		gi, ok := groupeditems[i.Op]
		if !ok {
			gi = []*QueryItem{i}
		} else {
			gi = append(gi, i)
		}
		groupeditems[i.Op] = gi
	}

	if _, ok := groupeditems[QueryFrom]; ok {
		fromItems := groupeditems[QueryFrom]
		for _, fromItem := range fromItems {
			b.This().SetConfig(ConfigKeyTableName, fromItem.Value.(string))
		}
	}

	if filter, ok := groupeditems[QueryWhere]; ok {
		translatedFilter, err := b.This().BuildFilter(filter[0].Value.(*Filter))
		if err != nil {
			return err
		}
		b.This().SetConfig(ConfigKeyWhere, translatedFilter)
		b.This().SetConfig(ConfigKeyFilter, filter[0].Value.(*Filter))
	}

	if _, ok := groupeditems[QuerySelect]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySelect)
		fields := groupeditems[QuerySelect][0].Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok := groupeditems[QueryAggr]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySelect)
	} else if _, ok = groupeditems[QueryInsert]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QueryInsert)
		fields := groupeditems[QueryInsert][0].Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok = groupeditems[QueryUpdate]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QueryUpdate)
		fields := groupeditems[QueryUpdate][0].Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok = groupeditems[QueryDelete]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QueryDelete)
	} else if _, ok = groupeditems[QuerySave]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySave)
	} else if _, ok = groupeditems[QuerySQL]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySQL)
	} else {
		b.This().SetConfig(ConfigKeyCommandType, QueryCommand)
	}
	b.This().SetConfig(ConfigKeyGroupedQueryItems, groupeditems)

	qop := b.Config(ConfigKeyCommandType, "")
	if qop == "" {
		return toolkit.Errorf("unable to build group query items. Invalid QueryOP is defined (%s)", qop)
	}
	return nil
}

func (b *QueryBase) SetThis(o IQuery) {
	b.self = o
}

func (b *QueryBase) This() IQuery {
	if b.self == nil {
		return b
	} else {
		return b.self
	}
}

func (b *QueryBase) BuildFilter(f *Filter) (interface{}, error) {
	return nil, toolkit.Error("Build filter is not yet implemented")
}

func (b *QueryBase) Cursor(in toolkit.M) ICursor {
	c := new(CursorBase)
	c.SetError(toolkit.Error("Cursor is not yet implemented"))
	return c
}

func (b *QueryBase) Execute(in toolkit.M) (interface{}, error) {
	return nil, toolkit.Error("Execute is not yet implemented")
}

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

/*
func (b *QueryBase) Reset() IQuery {
	b.items = []*QueryItem{}
	return b.This()
}

func (b *QueryBase) Select(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QuerySelect, fields})
	return b.This()
}

func (b *QueryBase) From(name string) IQuery {
	b.items = append(b.items, &QueryItem{QueryFrom, name})
	return b.This()
}

func (b *QueryBase) Where(f *Filter) IQuery {
	b.items = append(b.items, &QueryItem{QueryWhere, f})
	return b.This()
}

func (b *QueryBase) OrderBy(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QueryOrder, fields})
	return b.This()
}

func (b *QueryBase) GroupBy(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QueryGroup, fields})
	return b.This()
}

func (b *QueryBase) Aggr(aggritems ...*AggrItem) IQuery {
	b.items = append(b.items, &QueryItem{QueryAggr, aggritems})
	return b.This()
}

func (b *QueryBase) Insert(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QueryInsert, fields})
	return b.This()
}

func (b *QueryBase) Update(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QueryUpdate, fields})
	return b.This()
}

func (b *QueryBase) Delete() IQuery {
	b.items = append(b.items, &QueryItem{QueryDelete, true})
	return b.This()
}

func (b *QueryBase) Save() IQuery {
	b.items = append(b.items, &QueryItem{QuerySave, true})
	return b.This()
}

func (b *QueryBase) Take(n int) IQuery {
	b.items = append(b.items, &QueryItem{QueryTake, n})
	return b.This()
}

func (b *QueryBase) Skip(n int) IQuery {
	b.items = append(b.items, &QueryItem{QuerySkip, n})
	return b.This()
}

func (b *QueryBase) Command(command string, data interface{}) IQuery {
	b.items = []*QueryItem{&QueryItem{QueryCommand, toolkit.M{}.Set("command", command).Set("data", data)}}
	return b.This()
}

func (b *QueryBase) SQL(sql string) IQuery {
	b.items = []*QueryItem{&QueryItem{QuerySQL, sql}}
	return b.This()
}

func (b *QueryBase) Prepare() error {
	if !b.prepared {
		b.initConfig()
		gqis := b.BuildGroupedQueryItems()
		b.config.Set(ConfigKeyGroupedQueryItems, gqis)

		tablenames := []string{}
		if froms, ok := gqis[QueryFrom]; ok {
			for _, v := range froms {
				tablenames = append(tablenames, v.Value.(string))
			}
			b.config.Set(ConfigKeyTableName, tablenames)
		}

		filter, ok := gqis[QueryWhere]
		if ok {
			translatedFilter, err := b.This().BuildFilter(filter[0].Value.(*Filter))
			if err != nil {
				return err
			}
			b.config.Set(ConfigKeyWhere, translatedFilter)
		}

		cmd, err := b.This().BuildCommand()
		if err != nil {
			return err
		}
		b.config.Set(ConfigKeyCommand, cmd)
		b.prepared = true
	}
	return nil
}
*/

func (b *QueryBase) Cursor(in toolkit.M) ICursor {
	c := new(CursorBase)
	c.SetError(toolkit.Error("Cursor is not yet implemented"))
	return c
}

func (b *QueryBase) Execute(in toolkit.M) (interface{}, error) {
	return nil, toolkit.Error("Execute is not yet implemented")
}

/*
func (b *QueryBase) ExecCursor(in toolkit.M) ICursor {
	c := new(CursorBase)
	c.SetError(toolkit.Error("ExecCursor is not yet implemented"))
	return c
}

func (b *QueryBase) ExecQuery(in toolkit.M) (interface{}, error) {
	return nil, toolkit.Error("ExecQuery is not yet implemented")
}
*/

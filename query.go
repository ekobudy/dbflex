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
	ConfigKeyTableNames               = "tablenames"
)

type IQuery interface {
	/*
		BuildCommand(toolkit.M) (interface{}, error)
		BuildFilter(*Filter, toolkit.M) (interface{}, error)
		Prepare() ICursor
		Cursor(toolkit.M) ICursor
		Execute(toolkit.M) (interface{}, error)
	*/
	This() IQuery

	BuildCommand() (interface{}, error)
	BuildFilter(*Filter) (interface{}, error)
	Prepare() error
	Cursor(toolkit.M) ICursor
	Execute(toolkit.M) (interface{}, error)
	ExecCursor(toolkit.M) ICursor
	ExecQuery(toolkit.M) (interface{}, error)

	Reset() IQuery
	Select(...string) IQuery
	From(string) IQuery
	Where(*Filter) IQuery
	OrderBy(...string) IQuery
	GroupBy(...string) IQuery

	Aggr(...*AggrItem) IQuery
	Insert(...string) IQuery
	Update(...string) IQuery
	Delete() IQuery
	Save() IQuery

	Take(int) IQuery
	Skip(int) IQuery

	Command(string, interface{}) IQuery
	SQL(string) IQuery

	SetConfig(string, interface{})
	SetConfigM(toolkit.M)
	Config(string, interface{}) interface{}
	GetConfig(string, interface{}) interface{}
	DeleteConfig(...string)
}

type QueryBase struct {
	items []*QueryItem

	self        IQuery
	commandType QueryOp

	prepared bool

	config toolkit.M
}

type GroupedQueryItems map[QueryOp][]*QueryItem

func (q *QueryBase) initConfig() {
	if q.config == nil {
		q.config = toolkit.M{}
	}
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
	return q.GetConfig(key, def)
}

func (q *QueryBase) GetConfig(key string, def interface{}) interface{} {
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
	return nil, fmt.Errorf("Build command is not yet implemented")
}

func (b *QueryBase) BuildGroupedQueryItems() GroupedQueryItems {
	groupeditems := GroupedQueryItems{}
	for _, i := range b.items {
		gi, ok := groupeditems[i.Op]
		if !ok {
			gi = []*QueryItem{i}
		} else {
			gi = append(gi, i)
		}
		groupeditems[i.Op] = gi
	}

	if _, ok := groupeditems[QuerySelect]; ok {
		b.commandType = QuerySelect
		fields := groupeditems[QuerySelect][0].Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok := groupeditems[QueryAggr]; ok {
		b.commandType = QuerySelect
	} else if _, ok = groupeditems[QueryInsert]; ok {
		b.commandType = QueryInsert
		fields := groupeditems[QueryInsert][0].Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok = groupeditems[QueryUpdate]; ok {
		b.commandType = QueryUpdate
		fields := groupeditems[QueryUpdate][0].Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok = groupeditems[QueryDelete]; ok {
		b.commandType = QueryDelete
	} else if _, ok = groupeditems[QuerySave]; ok {
		b.commandType = QuerySave
	} else if _, ok = groupeditems[QuerySQL]; ok {
		b.commandType = QuerySQL
	} else {
		b.commandType = QueryCommand
	}

	return groupeditems
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

func (b *QueryBase) CommandType() QueryOp {
	return b.commandType
}

func (b *QueryBase) BuildFilter(f *Filter) (interface{}, error) {
	return nil, toolkit.Error("Build filter is not yet implemented")
}

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
			b.config.Set(ConfigKeyTableNames, tablenames)
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

func (b *QueryBase) Cursor(in toolkit.M) ICursor {
	b.This().Prepare()
	return b.This().ExecCursor(in)
}

func (b *QueryBase) Execute(in toolkit.M) (interface{}, error) {
	b.This().Prepare()
	return b.This().ExecQuery(in)
}

func (b *QueryBase) ExecCursor(in toolkit.M) ICursor {
	c := new(CursorBase)
	c.SetError(toolkit.Error("ExecCursor is not yet implemented"))
	return c
}

func (b *QueryBase) ExecQuery(in toolkit.M) (interface{}, error) {
	return nil, toolkit.Error("ExecQuery is not yet implemented")
}

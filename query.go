package dbflex

import (
	"github.com/eaciit/toolkit"
)

type IQuery interface {
	BuildFilter(*Filter, toolkit.M) (interface{}, error)

	Cursor(toolkit.M) ICursor
	Execute(toolkit.M) error

	Reset() IQuery
	Select(...string) IQuery
	From(string) IQuery
	Where(*Filter) IQuery
	OrderBy(...string) IQuery
	GroupBy(...string) IQuery

	Aggr(...*AggrItem) IQuery
	Insert() IQuery
	Update() IQuery
	Delete() IQuery
	Save() IQuery

	Take(int) IQuery
	Skip(int) IQuery

	Command(string, interface{}) IQuery
	SQL(string) IQuery
}

type QueryBase struct {
	items []*QueryItem

	self        IQuery
	commandType QueryOp
	Config      toolkit.M
}

type GroupedQueryItems map[QueryOp][]*QueryItem

func (b *QueryBase) GetGroupedQueryItems() GroupedQueryItems {
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
	} else if _, ok = groupeditems[QueryInsert]; ok {
		b.commandType = QueryInsert
	} else if _, ok = groupeditems[QueryUpdate]; ok {
		b.commandType = QueryUpdate
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

func (b *QueryBase) this() IQuery {
	if b.self == nil {
		return b
	} else {
		return b.self
	}
}

func (b *QueryBase) CommandType() QueryOp {
	return b.commandType
}

func (b *QueryBase) PrepareQuery(m toolkit.M) {
	b.Config = toolkit.M{}
	gqis := b.GetGroupedQueryItems()
	tablenames := []string{}
	if froms, ok := gqis[QueryFrom]; ok {
		for _, v := range froms {
			tablenames = append(tablenames, v.Value.(string))
		}
		b.Config.Set("tablenames", tablenames)
	}

	filter, ok := gqis[QueryWhere]
	if ok {
		translatedFilter, err := b.this().BuildFilter(filter[0].Value.(*Filter), m)
		if err == nil {
			b.Config.Set("where", translatedFilter)
		}
	}

	b.Config.Set("queryitems", gqis)
}

func (b *QueryBase) Cursor(m toolkit.M) ICursor {
	return nil
}

func (b *QueryBase) BuildFilter(f *Filter, in toolkit.M) (interface{}, error) {
	return nil, toolkit.Error("Build filter is not yet implemented")
}

func (b *QueryBase) Execute(m toolkit.M) error {
	//gqis := b.GetGroupedQueryItems()
	return toolkit.Error("execute is not yet implemented")
}

func (b *QueryBase) Reset() IQuery {
	b.items = []*QueryItem{}
	return b.this()
}

func (b *QueryBase) Select(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QuerySelect, fields})
	return b.this()
}

func (b *QueryBase) From(name string) IQuery {
	b.items = append(b.items, &QueryItem{QueryFrom, name})
	return b.this()
}

func (b *QueryBase) Where(f *Filter) IQuery {
	b.items = append(b.items, &QueryItem{QueryWhere, f})
	return b.this()
}

func (b *QueryBase) OrderBy(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QueryOrder, fields})
	return b.this()
}

func (b *QueryBase) GroupBy(fields ...string) IQuery {
	b.items = append(b.items, &QueryItem{QueryGroup, fields})
	return b.this()
}

func (b *QueryBase) Aggr(aggritems ...*AggrItem) IQuery {
	b.items = append(b.items, &QueryItem{QueryAggr, aggritems})
	return b.this()
}

func (b *QueryBase) Insert() IQuery {
	b.items = append(b.items, &QueryItem{QueryInsert, true})
	return b.this()
}

func (b *QueryBase) Update() IQuery {
	b.items = append(b.items, &QueryItem{QueryUpdate, true})
	return b.this()
}

func (b *QueryBase) Delete() IQuery {
	b.items = append(b.items, &QueryItem{QueryDelete, true})
	return b.this()
}

func (b *QueryBase) Save() IQuery {
	b.items = append(b.items, &QueryItem{QuerySave, true})
	return b.this()
}

func (b *QueryBase) Take(n int) IQuery {
	b.items = append(b.items, &QueryItem{QueryTake, n})
	return b.this()
}

func (b *QueryBase) Skip(n int) IQuery {
	b.items = append(b.items, &QueryItem{QuerySkip, n})
	return b.this()
}

func (b *QueryBase) Command(command string, data interface{}) IQuery {
	b.items = []*QueryItem{&QueryItem{QueryCommand, toolkit.M{}.Set("command", command).Set("data", data)}}
	return b.this()
}

func (b *QueryBase) SQL(sql string) IQuery {
	b.items = []*QueryItem{&QueryItem{QuerySQL, sql}}
	return b.this()
}

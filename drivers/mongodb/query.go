package mongodb

import (
	"fmt"
	"strings"

	"github.com/eaciit/toolkit"

	df "github.com/eaciit/dbflex"
	. "github.com/eaciit/toolkit"
)

type Query struct {
	df.QueryBase
	session *Session
}

func (q *Query) Cursor(m M) df.ICursor {
	q.PrepareQuery(m)
	cursor := new(Cursor)
	cursor.SetThis(cursor)

	tablenames := q.Config.Get("tablenames").([]string)
	coll := q.session.mgodb.C(tablenames[0])

	parts := q.Config.Get("queryitems").(df.GroupedQueryItems)
	where, hasWhere := M{}, q.Config.Has("where")
	if hasWhere {
		where = q.Config.Get("where").(M)
	}

	aggrs, hasAggr := parts[df.QueryAggr]
	groupby, hasGroup := parts[df.QueryGroup]

	if hasAggr {
		pipes := []M{}
		items := aggrs[0].Value.([]*df.AggrItem)
		aggrExpression := M{}
		for _, item := range items {
			if item.Op == df.AggrCount {
				aggrExpression.Set(item.Alias, M{}.Set(string(df.AggrSum), 1))
			} else {
				aggrExpression.Set(item.Alias, M{}.Set(string(item.Op), item.Field))
			}
		}
		if hasGroup {
			aggrExpression.Set("_id", "")
		} else {
			groups := func() M {
				s := M{}
				for _, v := range groupby {
					gs := v.Value.([]string)
					for _, g := range gs {
						if strings.TrimSpace(g) != "" {
							s.Set(strings.Replace(g, ".", "_", -1), "$"+g)
						}
					}
				}
				return s
			}()
			aggrExpression.Set("_id", groups)
		}

		if hasWhere {
			pipes = append(pipes, M{}.Set("$match", where))
		}
		pipes = append(pipes, M{}.Set("$group", aggrExpression))
		pipe := coll.Pipe(pipes).AllowDiskUse()
		cursor.isPipe = true
		cursor.mgopipe = pipe
		cursor.mgoiter = pipe.Iter()
	} else {
		qry := coll.Find(where)
		if items, ok := parts[df.QuerySelect]; ok {
			qry = qry.Select(items[0].Value.([]string))
		}

		if items, ok := parts[df.QueryOrder]; ok {
			qry = qry.Sort(items[0].Value.([]string)...)
		}

		if items, ok := parts[df.QuerySkip]; ok {
			qry = qry.Skip(items[0].Value.(int))
		}

		if items, ok := parts[df.QueryTake]; ok {
			qry = qry.Select(items[0].Value.(int))
		}

		cursor.mgocursor = qry
		cursor.mgoiter = qry.Iter()
	}
	return cursor
}

func (q *Query) Execute(m M) error {
	q.PrepareQuery(m)

	tablenames := q.Config.Get("tablenames").([]string)
	coll := q.session.mgodb.C(tablenames[0])
	data := m.Get("data")

	where, hasWhere := q.Config.Get("where"), q.Config.Has("where")

	switch ct := q.CommandType(); ct {
	case df.QueryInsert:
		return coll.Insert(data)

	case df.QueryUpdate:
		if hasWhere {
			_, err := coll.UpdateAll(where, data)
			return err
		} else {
			return toolkit.Errorf("update need to have where clause")
		}

	case df.QueryDelete:
		if hasWhere {
			_, err := coll.RemoveAll(where)
			return err
		} else {
			return toolkit.Errorf("delete need to have where clause")
		}

	case df.QuerySave:
		datam := M{}
		whereSave := M{}.Set("_id", "some-data-that-never-exist")
		err := toolkit.Serde(data, &datam, "json")
		if err != nil {
			return toolkit.Errorf("unable to deserialize data: %s", err.Error())
		}
		if datam.Has("_id") {
			whereSave = M{}.Set("_id", datam.Get("_id"))
		}
		_, err = coll.Upsert(whereSave, data)
		return err
	}

	return nil
}

func (q *Query) BuildFilter(f *df.Filter, in M) (interface{}, error) {
	fm := M{}
	if f.Op == df.OpEq {
		fm.Set(f.Field, f.Value)
	} else if f.Op == df.OpNe {
		fm.Set(f.Field, M{}.Set("$ne", f.Value))
	} else if f.Op == df.OpContains {
		fs := f.Value.([]string)
		if len(fs) > 1 {
			bfs := []interface{}{}
			for _, ff := range fs {
				pfm := M{}
				pfm.Set(f.Field, M{}.
					Set("$regex", fmt.Sprintf(".*%s.*", ff)).
					Set("$options", "i"))
				bfs = append(bfs, pfm)
			}
			fm.Set("$or", bfs)
		} else {
			fm.Set(f.Field, M{}.
				Set("$regex", fmt.Sprintf(".*%s.*", fs[0])).
				Set("$options", "i"))
		}
	} else if f.Op == df.OpStartWith {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf("^%s.*$", f.Value)).
			Set("$options", "i"))
	} else if f.Op == df.OpEndWith {
		fm.Set(f.Field, M{}.
			Set("$regex", fmt.Sprintf("^.*%s$", f.Value)).
			Set("$options", "i"))
	} else if f.Op == df.OpIn {
		fm.Set(f.Field, M{}.Set("$in", f.Value))
	} else if f.Op == df.OpNin {
		fm.Set(f.Field, M{}.Set("$nin", f.Value))
	} else if f.Op == df.OpGt {
		fm.Set(f.Field, M{}.Set("$gt", f.Value))
	} else if f.Op == df.OpGte {
		fm.Set(f.Field, M{}.Set("$gte", f.Value))
	} else if f.Op == df.OpLt {
		fm.Set(f.Field, M{}.Set("$lt", f.Value))
	} else if f.Op == df.OpLte {
		fm.Set(f.Field, M{}.Set("$lte", f.Value))
	} else if f.Op == df.OpOr || f.Op == df.OpAnd {
		bfs := []interface{}{}
		fs := f.Items
		for _, ff := range fs {
			bf, eb := q.BuildFilter(ff, in)
			if eb == nil {
				bfs = append(bfs, bf)
			}
		}

		fm.Set(string(f.Op), bfs)
	} else {
		return nil, fmt.Errorf("Filter Op %s is not defined", f.Op)
	}
	return fm, nil
}

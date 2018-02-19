package mongodb

import (
	"fmt"
	"strings"

	"github.com/eaciit/dbflex"

	"github.com/eaciit/toolkit"
	mgo "gopkg.in/mgo.v2"

	df "github.com/eaciit/dbflex"
	. "github.com/eaciit/toolkit"
)

type Query struct {
	df.QueryBase
	db       *mgo.Database
	prepared bool
}

func (q *Query) Reset() df.IQuery {
	q.QueryBase.Reset()
	q.prepared = false
	return q
}

func (q *Query) BuildCommand() (interface{}, error) {
	tablenames := q.GetConfig("tablenames", []string{}).([]string)
	if len(tablenames) == 0 {
		return nil, toolkit.Errorf("Table must be specified")
	}
	parts := q.GetConfig(df.ConfigKeyGroupedQueryItems, df.GroupedQueryItems{}).(df.GroupedQueryItems)

	//data, hasData := input["data"]
	where := q.GetConfig(df.ConfigKeyWhere, M{}).(M)
	//hasWhere := where != nil

	cmd := ""
	if items, ok := parts[df.QuerySelect]; ok {
		fields := strings.Join(items[0].Value.([]string), ",")
		if fields == "" {
			fields = "*"
		}
		cmd = toolkit.Sprintf("select %s", fields) + " from " + tablenames[0]

		if items, ok := parts[df.QueryTake]; ok {
			cmd += toolkit.Sprintf("limit %d", items[0].Value.(int))
		}

		if items, ok := parts[df.QuerySkip]; ok {
			cmd += toolkit.Sprintf("offset %d", items[0].Value.(int))
		}

		if items, ok := parts[df.QueryOrder]; ok {
			fields := []string{}
			for _, v := range items {
				orderfields := v.Value.([]string)
				for _, orderfield := range orderfields {
					if !strings.HasPrefix(orderfield, "-") {
						fields = append(fields, strings.TrimSpace(orderfield))
					} else {
						orderfield = orderfield[1:]
						fields = append(fields, strings.TrimSpace(orderfield)+" desc")
					}
				}
			}
			if len(fields) == 0 {
				cmd += "order by " + strings.Join(fields, ",")
			}
		}
	} else {
		//var data interface{}
		//hasData := false
		//fieldnames, _, _, sqlformats := ParseSQLMetadata(data)
		if items, ok = parts[df.QueryInsert]; ok {
			/*
				if !hasData {
					return nil, toolkit.Errorf("Non select command should have data")
				}
			*/
			//INSERT INTO table1 (ID, FullName, Email, Enable) VALUES (10, 'e10', 'e10', '1');
			//cmd = "insert into " + tablenames[0] + " (" + strings.Join(fieldnames, ",") + ") values (" + strings.Join(sqlformats, ",") + ")"
			q.SetConfig("fields", items[0].Value.([]string))
			cmd = "insert into " + tablenames[0] + " ({{fieldnames}}) values({{fieldvalues}})"
		} else if items, ok = parts[df.QueryUpdate]; ok {
			/*
				if !hasData {
					return nil, toolkit.Errorf("Non select command should have data")
				}
				requestedfields := items[0].Value.([]string)
				updatedFields := []string{}
				for i, v := range fieldnames {
					if len(requestedfields) == 0 {
						updatedFields = append(updatedFields, toolkit.Sprintf("%s=%s", v, sqlformats[i]))
					} else {
						lowerv := strings.ToLower(v)
						for _, f := range requestedfields {
							if strings.ToLower(f) == lowerv {
								updatedFields = append(updatedFields, toolkit.Sprintf("%s=%s", v, sqlformats[i]))
							}
						}
					}
				}
			*/
			//cmd = "update " + tablenames[0] + " set " + strings.Join(updatedFields, ",")
			q.SetConfig("fields", items[0].Value.([]string))
			cmd = "update " + tablenames[0] + " set {{updatedfields}}"
		} else if items, ok = parts[df.QueryDelete]; ok {
			cmd = "delete from " + tablenames[0]
		}
		if where != nil {
			//cmd += " where " + where
		}
	}
	//toolkit.Printf("Command is: %s", cmd)
	return cmd, nil
}

func (q *Query) BuildFilter(f *df.Filter) (interface{}, error) {
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
			bf, eb := q.BuildFilter(ff)
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

func (q *Query) Cursor(m M) df.ICursor {
	q.Prepare()
	cursor := new(Cursor)
	cursor.SetThis(cursor)

	tablenames := q.Config(df.ConfigKeyTableNames, []string{}).([]string)
	coll := q.db.C(tablenames[0])

	parts := q.Config(df.ConfigKeyGroupedQueryItems, df.GroupedQueryItems{}).(df.GroupedQueryItems)
	where := q.Config(df.ConfigKeyWhere, M{}).(M)
	hasWhere := where != nil

	aggrs, hasAggr := parts[df.QueryAggr]
	groupby, hasGroup := parts[df.QueryGroup]

	cq := new(Query)
	cq.SetThis(cq)
	cq.db = q.db
	cursor.SetCountQuery(cq)
	cq.Aggr(dbflex.NewAggrItem("Count", df.AggrCount, ""))
	cq.From(tablenames[0])
	if f, ok := parts[dbflex.QueryWhere]; ok {
		cq.Where(f[0].Value.(*dbflex.Filter))
	}

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

func (q *Query) Execute(m M) (interface{}, error) {
	q.Prepare()

	tablenames := q.Config(df.ConfigKeyTableNames, []string{}).([]string)
	coll := q.db.C(tablenames[0])
	data := m.Get("data")

	parts := q.Config(df.ConfigKeyGroupedQueryItems, df.GroupedQueryItems{}).(df.GroupedQueryItems)
	where := q.Config(df.ConfigKeyWhere, M{}).(M)
	hasWhere := where != nil

	switch ct := q.CommandType(); ct {
	case df.QueryInsert:
		return nil, coll.Insert(data)

	case df.QueryUpdate:
		var err error
		if hasWhere {
			//singleupdate := m.Get("singleupdate", true).(bool)
			singleupdate := false
			if !singleupdate {
				//-- get the field for update
				updateqi, _ := parts[df.QueryUpdate]
				updatevals := updateqi[0].Value.([]string)

				var dataM toolkit.M
				dataM, err = toolkit.ToM(data)
				dataS := toolkit.M{}
				if err != nil {
					return nil, err
				}

				if len(updatevals) > 0 {
					for k, v := range dataM {
						for _, u := range updatevals {
							if strings.ToLower(k) == strings.ToLower(u) {
								dataS[k] = v
							}
						}
					}
				} else {
					for k, v := range dataM {
						dataS[strings.ToLower(k)] = v
					}
				}
				updatedData := toolkit.M{}.Set("$set", dataS)

				_, err = coll.UpdateAll(where, updatedData)
			} else {
				err = coll.Update(where, data)
			}
			return nil, err
		} else {
			return nil, toolkit.Errorf("update need to have where clause")
		}

	case df.QueryDelete:
		if hasWhere {
			_, err := coll.RemoveAll(where)
			return nil, err
		} else {
			return nil, toolkit.Errorf("delete need to have where clause")
		}

	case df.QuerySave:
		whereSave := M{}.Set("_id", "some-data-that-never-exist")
		datam, err := toolkit.ToM(data)
		if err != nil {
			return nil, toolkit.Errorf("unable to deserialize data: %s", err.Error())
		}
		if datam.Has("_id") {
			whereSave = M{}.Set("_id", datam.Get("_id"))
		}
		_, err = coll.Upsert(whereSave, data)
		return nil, err
	}

	return nil, nil
}

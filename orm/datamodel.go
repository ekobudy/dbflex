package orm

import (
	"reflect"

	. "github.com/eaciit/dbflex"
	"github.com/eaciit/toolkit"
)

type DataModel interface {
	TableName() string
	Id() ([]string, []interface{})
	SetId([]interface{})

	PreSave()
	PostSave()
}

type DatamodelBase struct {
}

func (d *DatamodelBase) SetId(values []interface{}) {
	//-- do nothing
}

func (d *DatamodelBase) PreSave() {
	//-- do nothing
}

func (d *DatamodelBase) PostSave() {
	//-- do nothing
}

func Get(conn IConnection, model DataModel) error {
	tablename := model.TableName()
	where := generateFilterFromDataModel(conn, model)
	return conn.Cursor(From(tablename).Select().Where(where), toolkit.M{}).SetCloseAfterFetch().Fetch(model)
}

func Gets(conn IConnection, model DataModel, buffer interface{}, qp *QueryParam) error {
	tablename := model.TableName()

	cmd := From(tablename).Select()
	if qp != nil {
		if qp.Where != nil {
			cmd.Where(qp.Where)
		}

		if len(qp.Sort) > 0 {
			cmd.OrderBy(qp.Sort...)
		}

		if qp.Skip > 0 {
			cmd.Skip(qp.Skip)
		}

		if qp.Take > 0 {
			cmd.Take(qp.Take)
		}
	}

	err := conn.Cursor(cmd, nil).SetCloseAfterFetch().Fetchs(buffer, 0)
	return err
}

func Insert(conn IConnection, dm DataModel) error {
	tablename := dm.TableName()
	dm.PreSave()
	_, err := conn.Execute(
		From(tablename).Insert(),
		toolkit.M{}.Set("data", dm))
	if err != nil {
		dm.PostSave()
	}
	return err
}

func Save(conn IConnection, dm DataModel) error {
	tablename := dm.TableName()
	filter := generateFilterFromDataModel(conn, dm)

	dmexist := toolkit.M{}
	errexist := conn.Cursor(From(tablename).Where(filter), nil).
		SetCloseAfterFetch().
		Fetch(&dmexist)

	dm.PreSave()
	var err error
	if errexist == nil {
		_, err = conn.Execute(From(tablename).Where(filter).Update(),
			toolkit.M{}.Set("data", dm))
	} else {
		_, err = conn.Execute(From(tablename).Insert(),
			toolkit.M{}.Set("data", dm))
	}
	if err == nil {
		dm.PostSave()
	}
	return err
}

func Update(conn IConnection, dm DataModel) error {
	tablename := dm.TableName()
	filter := generateFilterFromDataModel(conn, dm)
	dm.PreSave()
	_, err := conn.Execute(
		From(tablename).Where(filter).Update(),
		toolkit.M{}.Set("data", dm))
	if err != nil {
		dm.PostSave()
	}
	return err
}

func Delete(conn IConnection, dm DataModel) error {
	tablename := dm.TableName()
	filter := generateFilterFromDataModel(conn, dm)
	_, err := conn.Execute(From(tablename).Where(filter).Delete(), nil)
	return err
}

func generateFilterFromDataModel(conn IConnection, dm DataModel) *Filter {
	fields, values := dm.Id()
	if len(fields) == 0 {
		return new(Filter)
	}

	fieldNameTag := conn.FieldNameTag()
	useTag := fieldNameTag != ""

	if useTag {
		vt := reflect.Indirect(reflect.ValueOf(dm)).Type()
		if len(fields) == 1 {
			if f, ok := vt.FieldByName(fields[0]); ok {
				fn := f.Tag.Get(fieldNameTag)
				if fn != "" {
					return Eq(fn, values[0])
				} else {
					return Eq(fields[0], values[0])
				}
			} else {
				return Eq(fields[0], values[0])
			}
		} else {
			eqs := []*Filter{}
			for idx, field := range fields {
				if f, ok := vt.FieldByName(field); ok {
					fn := f.Tag.Get(fieldNameTag)
					if fn != "" {
						eqs = append(eqs, Eq(fn, values[idx]))
					} else {
						eqs = append(eqs, Eq(field, values[idx]))
					}
				} else {
					eqs = append(eqs, Eq(field, values[idx]))
				}
				eqs = append(eqs, Eq(field, values[idx]))
			}
			return And(eqs...)
		}
	} else {
		if len(fields) == 1 {
			return Eq(fields[0], values[0])
		} else {
			eqs := []*Filter{}
			for idx, field := range fields {
				eqs = append(eqs, Eq(field, values[idx]))
			}
			return And(eqs...)
		}
	}
}

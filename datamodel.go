package dbflex

import (
	"github.com/eaciit/toolkit"
)

type IDataModel interface {
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

func Get(conn IConnection, model IDataModel) error {
	tablename := model.TableName()
	where := GenerateFilterFromDataModel(model)
	return conn.Cursor(From(tablename).Select().Where(where), toolkit.M{}).SetCloseAfterFetch().Fetch(model)
}

func Gets(conn IConnection, model IDataModel, buffer interface{}, qp *QueryParam) error {
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

func Insert(conn IConnection, dm IDataModel) error {
	tablename := dm.TableName()
	dm.PreSave()
	_, err := conn.Execute(From(tablename).Insert(), toolkit.M{}.Set("data", dm))
	if err != nil {
		dm.PostSave()
	}
	return err
}

func Save(conn IConnection, dm IDataModel) error {
	tablename := dm.TableName()
	filter := GenerateFilterFromDataModel(dm)

	dmexist := toolkit.M{}
	errexist := conn.Cursor(From(tablename).Where(filter), nil).SetCloseAfterFetch().Fetch(&dmexist)

	dm.PreSave()
	var err error
	if errexist == nil {
		_, err = conn.Execute(From(tablename).Where(filter).Update(), toolkit.M{}.Set("data", dm))
	} else {
		_, err = conn.Execute(From(tablename).Insert(), toolkit.M{}.Set("data", dm))
	}
	if err == nil {
		dm.PostSave()
	}
	return err
}

func Update(conn IConnection, dm IDataModel) error {
	tablename := dm.TableName()
	filter := GenerateFilterFromDataModel(dm)
	dm.PreSave()
	_, err := conn.Execute(From(tablename).Where(filter).Update(), toolkit.M{}.Set("data", dm))
	if err != nil {
		dm.PostSave()
	}
	return err
}

func Delete(conn IConnection, dm IDataModel) error {
	tablename := dm.TableName()
	filter := GenerateFilterFromDataModel(dm)
	_, err := conn.Execute(From(tablename).Where(filter).Delete(), nil)
	return err
}

func GenerateFilterFromDataModel(dm IDataModel) *Filter {
	fields, values := dm.Id()
	if len(fields) == 0 {
		return new(Filter)
	} else if len(fields) == 1 {
		return Eq(fields[0], values[0])
	} else {
		eqs := []*Filter{}
		for idx, f := range fields {
			eqs = append(eqs, Eq(f, values[idx]))
		}
		return And(eqs...)
	}
}

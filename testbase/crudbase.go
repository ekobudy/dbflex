package testbase

import (
	"testing"
	"time"

	"github.com/eaciit/dbflex"

	"github.com/eaciit/toolkit"
	. "github.com/smartystreets/goconvey/convey"
)

type employeeModel struct {
	ID       string `bson:"_id" json:"_id" sqlname:"id"`
	Name     string
	Grade    int
	JoinDate time.Time
	Salary   int
	Note     string
}

var buffer []employeeModel
var modelTableName = "employees"

func newEmployee(index int) interface{} {
	grade := toolkit.RandInt(19) + 1
	indexStr := toolkit.ToString(index)
	if len(indexStr) < 5 {
		prep := ""
		for i := 0; i < (5 - len(indexStr)); i++ {
			prep += "0"
		}
		indexStr = prep + indexStr
	}
	//toolkit.Printfn("Index: %s", indexStr)
	return &employeeModel{
		ID:       toolkit.Sprintf("EMP-%s", indexStr),
		Name:     toolkit.Sprintf("Employee Name: %d", index),
		Grade:    grade,
		JoinDate: time.Now().Add(-time.Duration(toolkit.RandInt(3000)*24) * time.Hour),
		Salary:   toolkit.RandInt(100*grade) + 500*grade,
		Note:     "",
	}
}

type CRUD struct {
	Model     interface{}
	TableName string
	NewData   func(int) interface{}

	config  toolkit.M
	t       *testing.T
	conntxt string
	conn    dbflex.IConnection
	count   int
}

func (c *CRUD) Set(key string, value interface{}) {
	if c.config == nil {
		c.config = toolkit.M{}
	}
	c.config.Set(key, value)
}

func (c *CRUD) Get(key string, def interface{}) interface{} {
	if c.config == nil {
		return def
	}
	return c.config.Get(key, def)
}

func NewCRUD(t *testing.T, ctxt string, count int, config toolkit.M) *CRUD {
	crud := new(CRUD)
	crud.t = t
	crud.conntxt = ctxt
	crud.count = count
	crud.config = config
	if config == nil {
		config = toolkit.M{}
	}

	if crud.Model == nil {
		crud.TableName = modelTableName
		crud.Model = employeeModel{}
		crud.NewData = newEmployee

		buffer = []employeeModel{}
		config.Set("buffer", &buffer)

		config.Set("deletefilter", dbflex.Eq("_id", "EMP-10"))

		config.Set("updatefilter", dbflex.Eq("grade", 5))
		config.Set("updatefields", []string{"note"})
		config.Set("updatedata", toolkit.M{}.Set("note", "This is updated data"))
		config.Set("updatevalidator", func() error {
			x := 0
			for _, v := range buffer {
				x++
				if v.Note != "This is updated data" {
					return toolkit.Errorf(toolkit.Sprintf("update error. data: %s",
						toolkit.JsonString(v)))
				}
			}
			if x == 0 {
				return toolkit.Error("no data being updated, length of buffer after populate is 0")
			}
			return nil
		})

		aggrbuffers := []struct {
			ID     string `bson:"_id" json:"_id"`
			Salary int
		}{}

		config.Set("aggrfilter", dbflex.Eq("grade", 3))
		config.Set("aggritems", []*dbflex.AggrItem{dbflex.Sum("salary")})
		config.Set("aggrbuffer", &aggrbuffers)
		config.Set("aggrvalidator", func() error {
			bufferTotal := 0
			if len(aggrbuffers) > 0 {
				bufferTotal = aggrbuffers[0].Salary
			}

			emps := []employeeModel{}
			err := crud.conn.Cursor(dbflex.From(crud.TableName).
				Where(dbflex.Eq("grade", 3)).
				Select(), nil).Fetchs(&emps, 0)
			if err != nil {
				return err
			}

			sal := 0
			for _, emp := range emps {
				sal += emp.Salary
			}

			if sal != bufferTotal {
				return toolkit.Errorf("aggr error | expect %d got %d", sal, bufferTotal)
			}
			return nil
		})
	}
	crud.config = config
	return crud
}

func (crud *CRUD) RunTest() {
	if crud.t == nil {
		return
	}

	if crud.Model == nil {
		crud.t.Fatalf("Model is nil. operation is cancelled")
		return
	}

	if crud.TableName == "" {
		crud.t.Fatalf("TableName is empty. operation is cancelled")
		return
	}

	Convey("Dbox test case", crud.t, func() {
		c, e := dbflex.NewConnectionFromUri(crud.conntxt, nil)
		Convey("Open connection should not be nil", func() {
			So(c, ShouldNotBeNil)
		})

		e = c.Connect()
		Convey("Connecting successfully", func() {
			So(e, ShouldBeNil)
		})

		if c == nil {
			return
		}
		defer close(c)

		crud.conn = c
		crud.clear()
		crud.insert()
		crud.populate()
		crud.delete()
		crud.update()
		crud.aggregate()
	})

	return
}

func (crud *CRUD) clear() {
	Convey("Clear table", func() {
		_, err := crud.conn.Execute(dbflex.From(crud.TableName).
			Delete(), nil)
		So(err, ShouldBeNil)
	})
}

func (crud *CRUD) insert() {
	Convey("Insert data", func() {
		//isErr := false
		query, err := crud.conn.Prepare(dbflex.From(crud.TableName).Insert())
		Convey("Prepare insert command", func() {
			So(err, ShouldBeNil)
		})

		Convey("Iterating insert command", func() {
			var err error
			for i := 0; i < crud.count; i++ {
				_, err = query.Execute(toolkit.M{}.Set("data", crud.NewData(i)))
				if err != nil {
					//isErr = true
					break
				}
			}
			So(err, ShouldBeNil)
		})
	})
}

func (crud *CRUD) populate() {
	Convey("Populate the data", func() {
		buffer := crud.config.Get("buffer")
		Convey("Buffer should not be nil", func() {
			So(buffer, ShouldNotBeNil)
		})

		err := crud.conn.Cursor(dbflex.From(crud.TableName).
			Select(), nil).
			Fetchs(buffer, 0)
		Convey("Fetching all data", func() {
			So(err, ShouldBeNil)
		})

		dataLength := toolkit.SliceLen(buffer)
		Convey(toolkit.Sprintf("Data returned should be %d", crud.count), func() {
			So(dataLength, ShouldEqual, crud.count)
		})

		Convey("Get Count", func() {
			cursor := crud.conn.Cursor(dbflex.From(crud.TableName).Select(), nil)
			count := cursor.Count()
			Convey("Count cursor has no error", func() {
				So(cursor.Error(), ShouldBeNil)
			})

			Convey("Count result is OK", func() {
				So(count, ShouldEqual, crud.count)
			})
		})

		Convey("Get eq data", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.Eq("grade", 4))
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))
			So(models[0].Grade, ShouldEqual, 4)
		})

		Convey("Get ne data", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.Ne("grade", 4))
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))
			So(models[0].Grade, ShouldNotEqual, 4)

		})

		Convey("Get contains data - single", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.Contains("name", "535"))
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))
			So((models)[0].Name, ShouldContainSubstring, "535")
		})

		Convey("Get contains data - plural", func() {
			cmd := dbflex.From(crud.TableName).Select().
				Where(dbflex.Contains("name", "535", "536")).
				OrderBy("name")
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))

			hash := toolkit.Sprintf("%s-%s",
				models[0].Name[len(models[0].Name)-3:],
				models[1].Name[len(models[0].Name)-3:])
			So(hash, ShouldEqual, "535-536")
		})

		Convey("Get or data", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.Or(
				dbflex.Contains("name", "535"), dbflex.Contains("name", "536"))).
				OrderBy("name")
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))

			hash := toolkit.Sprintf("%s-%s",
				models[0].Name[len(models[0].Name)-3:],
				models[1].Name[len(models[0].Name)-3:])
			So(hash, ShouldEqual, "535-536")
		})

		Convey("Get and data - include gt", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.And(
				dbflex.Eq("grade", 4), dbflex.Gt("salary", 2200))).
				OrderBy("name")
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))

			hash := toolkit.Sprintf("%d-%d", models[0].Grade, models[0].Salary)
			So(hash, ShouldBeGreaterThan, "4-2200")
		})

		Convey("Get order data descending", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.Or(
				dbflex.Contains("name", "535"), dbflex.Contains("name", "536"))).
				OrderBy("-name")
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))

			So(models[0].Name, ShouldContainSubstring, "536")
		})

		Convey("Get data take", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.Eq("grade", 3)).
				OrderBy("-name").Take(2)
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 0)
			models := *(buffer.(*[]employeeModel))
			dataLength := len(models)
			grade3 := true
			for _, model := range models {
				if model.Grade != 3 {
					grade3 = false
				}
			}
			So(dataLength, ShouldEqual, 2)
			So(grade3, ShouldBeTrue)
		})

		Convey("Get data partially", func() {
			cmd := dbflex.From(crud.TableName).Select().Where(dbflex.Eq("grade", 3)).
				OrderBy("-name")
			crud.conn.Cursor(cmd, nil).Fetchs(buffer, 2)
			models := *(buffer.(*[]employeeModel))
			dataLength := len(models)
			grade3 := true
			for _, model := range models {
				if model.Grade != 3 {
					grade3 = false
				}
			}
			So(dataLength, ShouldEqual, 2)
			So(grade3, ShouldBeTrue)
		})
	})
}

func (crud *CRUD) delete() {
	Convey("Delete data", func() {
		where := crud.config.Get("deletefilter", &dbflex.Filter{}).(*dbflex.Filter)
		buffer := crud.config.Get("buffer")
		Convey("Buffer should not be nil", func() {
			So(buffer, ShouldNotBeNil)
		})

		Convey("Deleting data", func() {
			_, err := crud.conn.Execute(
				dbflex.From(crud.TableName).Where(where).Delete(), nil)
			So(err, ShouldBeNil)

			Convey("Populate data after data being deleted", func() {
				err := crud.conn.Cursor(dbflex.From(crud.TableName).Select().
					Where(where), nil).
					Fetchs(buffer, 0)
				So(err, ShouldBeNil)

				Convey(toolkit.Sprintf("Data returned should be %d", 0), func() {
					dataLength := toolkit.SliceLen(buffer)
					So(dataLength, ShouldEqual, 0)
				})
			})
		})
	})
}

func (crud *CRUD) update() {
	Convey("Update data", func() {
		where := crud.config.Get("updatefilter", new(dbflex.Filter)).(*dbflex.Filter)
		fieldsToUpdate := crud.config.Get("updatefields", []string{}).([]string)
		data := crud.config.Get("updatedata", nil)
		buffer := crud.config.Get("buffer")
		validator := crud.config.Get("updatevalidator", func() error { return nil }).(func() error)

		Convey("Updating data", func() {
			_, err := crud.conn.Execute(dbflex.From(crud.TableName).Where(where).
				Update(fieldsToUpdate...), toolkit.M{}.Set("data", data))
			So(err, ShouldBeNil)

			Convey("Populating data after update", func() {
				err := crud.conn.Cursor(dbflex.From(crud.TableName).Select().
					Where(where), nil).
					Fetchs(buffer, 0)
				So(err, ShouldBeNil)

				Convey("Validate data after data update", func() {
					err = validator()
					So(err, ShouldBeNil)
				})
			})
		})

	})
}

func (crud *CRUD) aggregate() {
	Convey("Aggregation", func() {
		where := crud.config.Get("aggrfilter", new(dbflex.Filter)).(*dbflex.Filter)
		aggrItems := crud.config.Get("aggritems", []*dbflex.AggrItem{}).([]*dbflex.AggrItem)
		aggrGroup := crud.config.Get("aggrgroup", []string{}).([]string)
		buffer := crud.config.Get("aggrbuffer")
		validator := crud.config.Get("aggrvalidator", func() error { return nil }).(func() error)

		Convey("Execute aggregation", func() {
			cursor := crud.conn.Cursor(dbflex.From(crud.TableName).
				Where(where).GroupBy(aggrGroup...).
				Aggr(aggrItems...), nil)
			Convey("Cursor has no error", func() {
				So(cursor.Error(), ShouldBeNil)
			})

			Convey("Fetch aggr cursor", func() {
				err := cursor.Fetchs(buffer, 0)
				So(err, ShouldBeNil)
			})
		})

		Convey("Validate", func() {
			err := validator()
			So(err, ShouldBeNil)
		})
	})
}

func openConn(ctxt string) dbflex.IConnection {
	c, e := dbflex.NewConnectionFromUri(ctxt, nil)
	Convey("Open connection", func() {
		Convey("Connection should not be nil", func() {
			So(c, ShouldNotBeNil)
		})

		e = c.Connect()
		Convey("Connecting successfully", func() {
			So(e, ShouldBeNil)
		})
	})
	return c
}

func close(c dbflex.IConnection) {
	c.Close()
}

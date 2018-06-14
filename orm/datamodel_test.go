package orm

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eaciit/dbflex"

	"github.com/eaciit/toolkit"

	. "github.com/eaciit/dbflex"
	. "github.com/smartystreets/goconvey/convey"

	_ "github.com/eaciit/dbflex/drivers/mongodb"
)

type fakeModel struct {
	DatamodelBase `bson:"-"`
	ID            string `json:"_id" bson:"_id"`
	Title         string
	IntValue      int
	DateValue     time.Time
	Created       time.Time
	CreatedBy     string
	LastUpdate    time.Time
	LastUpdateBy  string
}

func newFake() *fakeModel {
	fm := new(fakeModel)
	fm.ID = toolkit.RandomString(20)
	fm.Title = fm.ID
	fm.IntValue = toolkit.RandInt(900) + 100
	fm.DateValue = time.Now().AddDate(0, 0, -toolkit.RandInt(2*365))
	return fm
}

const (
	connTxt = "mongodb://localhost:27123/dbtest"
)

func (fm *fakeModel) TableName() string {
	return "faketable"
}

func (fm *fakeModel) Id() ([]string, []interface{}) {
	return []string{"ID"}, []interface{}{fm.ID}
}

func (fm *fakeModel) SetId(ids []interface{}) {
	fm.ID = ids[0].(string)
}

func (fm *fakeModel) PreSave() {
	fm.LastUpdate = time.Now()
}

var conn IConnection

var deletedId, updatedId string

func TestConnect(t *testing.T) {
	Convey("Connect and clear table", t, func() {
		var err error

		conn, err = NewConnectionFromUri(connTxt, nil)
		Convey("Prepare connection", func() {
			So(err, ShouldBeNil)
		})

		err = conn.Connect()
		Convey("Connect", func() {
			So(err, ShouldBeNil)
		})

		Convey("Clear table", func() {
			_, err = conn.Execute(From(new(fakeModel).TableName()).Delete(), nil)
			So(err, ShouldBeNil)
		})
	})
}

func TestPrepareDataModel(t *testing.T) {
	Convey("Prepare Data Model", t, func() {
		errs := []string{}
		if conn != nil {
			Convey("Insert data", func() {
				for i := 0; i < 1000; i++ {
					fm := newFake()
					err := Insert(conn, fm)
					if err != nil {
						errs = append(errs, err.Error())
					} else {
						if i == 100 {
							deletedId = fm.ID
						}

						if i == 500 {
							updatedId = fm.ID
						}
					}
				}
			})
		}

		errFull := strings.Join(errs, "\n")
		So(errFull, ShouldBeBlank)
	})
}

func TestReading(t *testing.T) {
	Convey("Applying Reading operation on Data Model", t, func() {
		Convey("Reading all data into buffers", func() {
			var models []*fakeModel
			err := Gets(conn, new(fakeModel), &models, nil)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data length is 1000", func() { So(len(models), ShouldEqual, 1000) })
		})

		Convey("Reading only intvalue between 101 and 200 - Filter", func() {
			var models []*fakeModel
			err := Gets(conn, new(fakeModel), &models, &QueryParam{
				Where: dbflex.Range("intvalue", 101, 200),
			})
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data length > 0", func() { So(len(models), ShouldBeGreaterThan, 0) })
			if len(models) > 0 {
				Convey("IntValue between 101 and 200", func() {
					var err error
					for _, m := range models {
						if m.IntValue < 101 || m.IntValue > 200 {
							err = toolkit.Errorf("Data: %s", toolkit.JsonString(m))
						}
					}
					So(err, ShouldBeNil)
				})
			}
		})

		Convey("Reading first 5 by date - Sort, Take, Skip", func() {
			var models []*fakeModel
			lastDate := toolkit.ToDate("2000-01-01", "YYYY-MM-dd")
			err := Gets(conn, new(fakeModel), &models, &QueryParam{
				Take: 5,
				Sort: []string{"datevalue"},
			})
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data length = 5", func() { So(len(models), ShouldEqual, 5) })
			Convey("Date is in order", func() {
				var err error
				for _, m := range models {
					if m.DateValue.Before(lastDate) {
						err = toolkit.Errorf("%v is before %v", m.DateValue, lastDate)
					} else {
						lastDate = m.DateValue
					}
				}
				So(err, ShouldBeNil)

				Convey("Get another 10 rows", func() {
					err := Gets(conn, new(fakeModel), &models, &QueryParam{
						Skip: 5,
						Take: 10,
						Sort: []string{"datevalue"},
					})
					Convey("No error", func() { So(err, ShouldBeNil) })
					Convey("Data length = 10", func() { So(len(models), ShouldEqual, 10) })
					Convey("Date is in order", func() {
						dates := []string{}
						var err error
						for _, m := range models {
							if m.DateValue.Before(lastDate) {
								err = toolkit.Errorf("%v is before %v", m.DateValue, lastDate)
							} else {
								lastDate = m.DateValue
								dates = append(dates, toolkit.Date2String(lastDate, "dd-MMM"))
							}
						}
						So(err, ShouldBeNil)
						//toolkit.Printfn("\n%s", toolkit.JsonString(dates))
					})
				})
			})
		})

		Convey("Get only 1 record", func() {
			fm := new(fakeModel)
			fm.ID = updatedId
			err := Get(conn, fm)

			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data returned is right", func() {
				So(fm.ID, ShouldEqual, updatedId)
				So(fm.Title, ShouldNotEqual, "")
			})
		})
	})
}

func TestDelete(t *testing.T) {
	Convey("Delete data", t, func() {
		Convey("Delete", func() {
			fm := new(fakeModel)
			fm.ID = deletedId
			err := Delete(conn, fm)
			So(err, ShouldBeNil)

			Convey("Validate process", func() {
				var buffers []*fakeModel
				_ = Gets(conn, new(fakeModel), &buffers, nil)
				So(len(buffers), ShouldEqual, 999)
			})
		})
	})
}

func TestUpdate(t *testing.T) {
	Convey("Update data", t, func() {
		Convey("Update", func() {
			fm := new(fakeModel)
			fm.ID = updatedId
			_ = Get(conn, fm)
			fm.Title = "This data has been updated"
			err := Update(conn, fm)
			So(err, ShouldBeNil)

			Convey("Validate process", func() {
				fm := new(fakeModel)
				fm.ID = updatedId
				_ = Get(conn, fm)
				So(fm.ID, ShouldEqual, updatedId)
				So(fm.Title, ShouldEqual, "This data has been updated")
			})
		})
	})
}

func TestSave(t *testing.T) {
	Convey("Save data", t, func() {
		Convey("Build new datalist", func() {
			datas := []*fakeModel{}

			fm := new(fakeModel)
			fm.ID = updatedId
			err := Get(conn, fm)
			if err == nil {
				fm.Title = "Saved data"
				datas = append(datas, fm)
			}

			for i := 0; i < 10; i++ {
				fm = newFake()
				fm.ID = toolkit.Sprintf("SaveData%d", i+1)
				fm.Title = "Saved data"
				datas = append(datas, fm)
			}
			So(len(datas), ShouldEqual, 11)

			Convey("Save", func() {
				errors := []string{}
				for _, data := range datas {
					err := Save(conn, data)
					if err != nil {
						errors = append(errors, err.Error())
					}
				}
				So(len(errors), ShouldEqual, 0)

				Convey("Validate 1 - all data shld be 1009", func() {
					var buffers []*fakeModel
					_ = Gets(conn, new(fakeModel), &buffers, nil)
					So(len(buffers), ShouldEqual, 1009)
				})

				Convey("Validate 2 - saved data shld be 11", func() {
					var buffers []*fakeModel
					_ = Gets(conn, new(fakeModel), &buffers, &QueryParam{Where: dbflex.Eq("title", "Saved data")})
					So(len(buffers), ShouldEqual, 11)
				})
			})
		})
	})
}

func TestInsertUsingPooling(t *testing.T) {
	pooling := dbflex.NewDbPooling(10, func() (dbflex.IConnection, error) {
		conn, err := NewConnectionFromUri(connTxt, nil)
		if err != nil {
			return nil, err
		}

		err = conn.Connect()
		if err != nil {
			return nil, err
		}

		return conn, nil
	})
	pooling.Timeout = 5 * time.Second
	cmodel := make(chan *fakeModel)
	defer pooling.Close()

	//cout := make(chan string)
	wg := new(sync.WaitGroup)
	go func() {
		for wi := 0; wi < 20; wi++ {
			go func() {
				errors := []string{}
				for model := range cmodel {
					func() {
						defer wg.Done()
						pconn, err := pooling.Get()
						if err != nil {
							errors = append(errors, toolkit.Sprintf("unable to get connection. %s", err.Error()))
						} else {
							defer pconn.Release()
							err = Save(pconn.Connection(), model)
							if err != nil {
								errors = append(errors, toolkit.Sprintf("unable to save data. %s", err.Error()))
							}

							//--- give some delay yo ensure pooling queue process taken place
							time.Sleep(time.Duration(toolkit.RandInt(10)) * time.Millisecond)
						}
					}()
				}
				//cout <- strings.Join(errors, "\n")
			}()
		}
	}()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		fm := newFake()
		fm.ID = toolkit.Sprintf("pooling-%d", i)
		fm.Title = "This user is saved using DB Pool"
		cmodel <- fm
	}
	close(cmodel)

	wg.Wait()
}

func TestClose(t *testing.T) {
	conn.Close()
}

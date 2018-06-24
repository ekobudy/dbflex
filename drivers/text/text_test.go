package text

import (
	"math"
	"testing"
	"time"

	"github.com/eaciit/toolkit"

	"github.com/eaciit/dbflex/testbase"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	cfg = NewTextObjSetting(',').
		SetUseSign(true).SetSign('"').
		SetDateFormat("", "yyyy-MM-dd HH:mm:ss")
)

func TestTextToObjField(t *testing.T) {
	Convey("Cast a text to object field", t, func() {
		type fakeModel struct {
			ID          string `flex:"id"`
			Title       string
			NumberInt   int
			NumberFloat float64
			Created     time.Time
		}

		fm := new(fakeModel)

		Convey("Set string", func() {
			err := processTxtToObjField("Record1", fm, "id", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() { So(fm.ID, ShouldEqual, "Record1") })
		})

		Convey("Set float", func() {
			err := processTxtToObjField("30.5", fm, "numberfloat", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() { So(fm.NumberFloat, ShouldEqual, 30.5) })
		})

		Convey("Set Date", func() {
			txt := "2018-06-01 00:00:00"
			dt := toolkit.ToDate(txt, "yyyy-MM-dd hh:mm:ss")
			err := processTxtToObjField(txt, fm, "created", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() {
				diff := math.Abs(float64(fm.Created.Sub(dt)))
				So(diff, ShouldBeLessThanOrEqualTo, float64(1*time.Second))
			})
		})
	})
}

func TestTextToMapField(t *testing.T) {
	Convey("Cast a text to map field", t, func() {
		fm := toolkit.M{}

		Convey("Set string", func() {
			err := processTxtToObjField("Record1", fm, "ID", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() { So(fm.GetString("ID"), ShouldEqual, "Record1") })
		})

		Convey("Set float", func() {
			err := processTxtToObjField("30.5", fm, "NumberFloat", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() { So(fm.GetFloat64("NumberFloat"), ShouldEqual, 30.5) })
		})

		Convey("Set Date", func() {
			txt := "2018-06-01 00:00:00"
			dt := toolkit.ToDate(txt, "yyyy-MM-dd hh:mm:ss")
			err := processTxtToObjField(txt, fm, "Created", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() {
				diff := math.Abs(float64(fm.Get("Created", time.Now()).(time.Time).Sub(dt)))
				So(diff, ShouldBeLessThanOrEqualTo, float64(1*time.Second))
			})
		})
	})
}

func TestTextToObj(t *testing.T) {
	Convey("Cast a text to object", t, func() {
		type fakeModel struct {
			ID          string
			Title       string
			NumberInt   int
			NumberFloat float64
			Created     time.Time
		}

		data := "\"Record1\",\"Title untuk Record 1\",30,20.5,\"2018-06-15 10:00:00\""

		Convey("To Obj", func() {
			fm := new(fakeModel)
			err := textToObj(data, fm, cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Valid data", func() {
				So(fm.ID, ShouldEqual, "Record1")
				So(fm.NumberInt, ShouldEqual, 30)
				So(fm.NumberFloat, ShouldEqual, 20.5)

				date1txt := "2018-06-15 10:00:00"
				date2txt := toolkit.Date2String(fm.Created, cfg.DateFormat(""))
				So(date1txt, ShouldEqual, date2txt)
			})
		})

		Convey("To M", func() {
			m := toolkit.M{}
			err := textToObj(data, &m, cfg, "ID", "Title", "NumberInt", "NumberFloat", "Created")
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Valid data", func() {
				So(m.GetString("ID"), ShouldEqual, "Record1")
				So(m.GetInt("NumberInt"), ShouldEqual, 30)

				float := m.GetFloat32("NumberFloat")
				So(float, ShouldEqual, 20.5)

				date1txt := "2018-06-15 10:00:00"
				date2txt := toolkit.Date2String(m.Get("Created", time.Now()).(time.Time), cfg.DateFormat(""))
				So(date1txt, ShouldEqual, date2txt)
			})
		})
	})
}
func TestCRUD(t *testing.T) {
	workpath := "/Users/ariefdarmawan/Go/src/github.com/eaciit/dbflex/data"
	crud := testbase.NewCRUD(t, toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath),
		100,
		toolkit.M{}.Set("conn_config", toolkit.M{}.Set("text_object_setting", cfg)))
	crud.RunTest("clear", "insert", "read")
}

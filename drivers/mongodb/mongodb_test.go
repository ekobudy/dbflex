package mongodb

import (
	"testing"

	"github.com/eaciit/dbflex"
	"github.com/eaciit/toolkit"
)

const (
	sqlconnectionstring = "localhost:27123/dbtest"
	tablename           = "dftable1"
)

var (
	conn dbflex.IConnection
)

func TestConnect(t *testing.T) {
	conn = dbflex.NewConnectionFromUri("mongodb", sqlconnectionstring, nil)
	if err := conn.Connect(); err != nil {
		t.Fatal(err)
	}
}

func TestClearTable(t *testing.T) {
	_, err := conn.NewQuery().From(tablename).Delete().Execute(nil)
	check(t, err, "fail to clear table")
}

func TestInsert(t *testing.T) {
	for i := 0; i < 5; i++ {
		_, err := conn.NewQuery().From(tablename).Insert().Execute(toolkit.M{}.Set("data", toolkit.M{}.Set("_id", i+1).
			Set("FullName", toolkit.Sprintf("FN %d", i+1)).
			Set("Email", toolkit.Sprintf("em%d@mail.com", i+1)).
			Set("Enable", 1)))
		check(t, err, "unable to insert")
	}

	recordcount := struct {
		N int
	}{}
	err := conn.NewQuery().From(tablename).Select("count(*) as N").Cursor(nil).Fetch(&recordcount)
	check(t, err, "unable to detect count")
	if recordcount.N != 5 {
		check(t, toolkit.Errorf("Expected %d records got %d", 5, recordcount.N), "")
	}
}

func TestUpdate(t *testing.T) {
	_, err := conn.NewQuery().From(tablename).
		Where(dbflex.Eq("FullName", "FN 3")).
		Update("email", "enable").
		Execute(toolkit.M{}.Set("data", toolkit.M{}.
			//Set("_id", 1).
			//Set("FullName", "ED 3").
			Set("Email", "em3@eaciit.com").
			Set("Enable", 0)))
	check(t, err, "update")

	m := toolkit.M{}
	err = conn.NewQuery().From(tablename).Select().Where(dbflex.Eq("FullName", "FN 3")).Cursor(nil).Fetch(&m)
	check(t, err, "fetch update result")
	email := m.Get("Email", "").(string)
	if email != "em3@eaciit.com" {
		check(t, toolkit.Errorf("got %s", email), "update result error")
	}
}
func TestDelete(t *testing.T) {
	_, err := conn.NewQuery().From(tablename).Where(dbflex.Eq("FullName", "FN 4")).
		Delete().Execute(nil)
	check(t, err, "delete")

	c := conn.NewQuery().From(tablename).Select().Where(dbflex.Eq("FullName", "FN 4")).Cursor(nil)
	check(t, c.Error(), "fetch delete result")
	if c.Count() != 0 {
		check(t, toolkit.Errorf("Expected %d records got %d", 0, c.Count()), "")
	}
}

func TestClose(t *testing.T) {
	conn.Close()
}

type person struct {
	dbflex.DatamodelBase
	ID       int    `autonumber:"yes" bson:"_id" json:"_id"`
	FullName string `label:"@FNLabel" tooltip:"asdasda" mandatory:"yes" column:3 `
	Enable   bool
}

func (p *person) TableName() string {
	return tablename
}

func (p *person) Id() ([]string, []interface{}) {
	return []string{"_id"}, []interface{}{p.ID}
}

func check(t *testing.T, err error, hdr string) {
	if err != nil {
		if hdr == "" {
			t.Fatal(err.Error())
		} else {
			t.Fatalf(hdr + ":" + err.Error())
		}
	}
}

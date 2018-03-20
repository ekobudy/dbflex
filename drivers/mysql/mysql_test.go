package mysql

import (
	"testing"

	"github.com/eaciit/dbflex"
	"github.com/eaciit/toolkit"
)

const (
	sqlconnectionstring = "mysql://root:Database.1@localhost:3306/ectestdb"
)

var (
	conn dbflex.IConnection
)

func TestConnect(t *testing.T) {
	conn = dbflex.NewConnectionFromUri(sqlconnectionstring, nil)
	if err := conn.Connect(); err != nil {
		t.Fatal(err)
	}
}

func TestClearTable(t *testing.T) {
	_, err := conn.NewQuery().From("table1").Delete().Execute(nil)
	check(t, err, "fail to clear table")
}

func TestInsert(t *testing.T) {
	for i := 0; i < 5; i++ {
		_, err := conn.NewQuery().From("table1").Insert().Execute(toolkit.M{}.Set("data",
			toolkit.M{}.Set("ID", i+1).
				Set("FullName", toolkit.Sprintf("FN %d", i+1)).
				Set("Email", toolkit.Sprintf("em%d@mail.com", i+1)).
				Set("Enable", 1)))
		check(t, err, "unable to insert")
	}

	recordcount := struct {
		N int
	}{}
	err := conn.NewQuery().From("table1").Select("count(*) as N").Cursor(nil).Fetch(&recordcount)
	check(t, err, "unable to detect count")
	if recordcount.N != 5 {
		check(t, toolkit.Errorf("Expected %d records got %d", 5, recordcount.N), "")
	}
}

func TestCount(t *testing.T) {
	c := conn.NewQuery().From("table1").Select().Cursor(nil)
	check(t, c.Error(), "unable to initiate query")

	count := c.Count()
	if count != 5 {
		t.Fatalf("expect %d got %d", 5, count)
	}
	check(t, c.Error(), "unable to initiate query")
}

func TestUpdate(t *testing.T) {
	_, err := conn.NewQuery().From("table1").Where(dbflex.Eq("FullName", "FN 3")).
		Update("email", "enable").
		Execute(toolkit.M{}.Set("data", toolkit.M{}.
			Set("ID", 1).
			//Set("FullName", "ED 3").
			Set("Email", "em3@eaciit.com").
			Set("Enable", 0)))
	check(t, err, "update")

	m := toolkit.M{}
	err = conn.NewQuery().From("table1").Select().Where(dbflex.Eq("FullName", "FN 3")).Cursor(nil).Fetch(&m)
	check(t, err, "fetch update result")
	email := m.Get("Email", "").(string)
	if email != "em3@eaciit.com" {
		check(t, toolkit.Errorf("got %s", email), "update result error")
	}
}
func TestDelete(t *testing.T) {
	_, err := conn.NewQuery().From("table1").Where(dbflex.Eq("FullName", "FN 4")).
		Delete().Execute(nil)
	check(t, err, "delete")

	m := struct{ N int }{}
	err = conn.NewQuery().From("table1").Select("count(*) as N").Where(dbflex.Eq("FullName", "FN 4")).
		Cursor(nil).Fetch(&m)
	check(t, err, "fetch delete result")
	if m.N != 0 {
		t.Fatalf("delete fail. existing record %v", m.N)
	}
}

func TestClose(t *testing.T) {
	conn.Close()
}

type person struct {
	dbflex.DatamodelBase
	ID              int `autonumber:"yes"`
	FullName, Email string
	Enable          bool
}

func (p *person) TableName() string {
	return "table1"
}

// IDInfo provides ID information of an object
func (p *person) IDInfo() ([]string, []interface{}) {
	return []string{"ID"}, []interface{}{p.ID}
}

func check(t *testing.T, err error, hdr string) {
	if err != nil {
		if hdr == "" {
			t.Fatal(err.Error())
		} else {
			t.Fatalf("%s: %s", hdr, err.Error())
		}
	}
}

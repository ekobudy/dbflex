package test

import (
	"fmt"
	"testing"

	"github.com/eaciit/dbflex"
	_ "github.com/eaciit/dbflex/drivers/mongodb"
	"github.com/eaciit/toolkit"
)

var (
	conn   dbflex.IConnection
	sess   dbflex.ISession
	query  dbflex.IQuery
	cursor dbflex.ICursor
	err    error
)

type datamodel struct {
	ID    string `bson:"_id" json:"_id"`
	Name  string
	Level string
}

func TestConnect(t *testing.T) {
	conn = dbflex.NewConnectionFromUri("mongodb", "localhost:27123/dbtest", toolkit.M{}.Set("timeout", 3))
	if err = conn.Connect(); err != nil {
		t.Fatalf("unable to connect: %s", err.Error())
	}
}

func TestObjectNames(t *testing.T) {
	objnames := conn.ObjectNames(dbflex.ObjTypeAll)
	fmt.Printf("Object retrieves: %d\n", len(objnames))
	for _, v := range objnames {
		fmt.Printf("%v\n", v)
	}
}

func TestSession(t *testing.T) {
	if sess, err = conn.NewSession(); err != nil {
		t.Fatalf("unable to initiate new session: %s", err.Error())
	}
	//defer sess.Close()
}

func TestFetch(t *testing.T) {
	query = sess.NewQuery()
	query.From("tabletest").Select().Where(dbflex.EndWith("_id", "01"))
	cursor = query.Cursor(nil).SetCloseAfterFetch()

	result := toolkit.M{}
	err = cursor.Fetch(&result)
	if err != nil {
		t.Fatalf("unable to fetch: %s", err.Error())
	} else {
		fmt.Println("Record count: ", cursor.Count())
		fmt.Println("Data ", 1, ":", toolkit.JsonString(result))
	}
}

func TestAggr(t *testing.T) {
	query = sess.NewQuery()
	query.From("tabletest").Select().Aggr(dbflex.NewAggrItem("c", dbflex.AggrCount, ""))
	cursor = query.Cursor(nil).SetCloseAfterFetch()

	result := toolkit.M{}
	err = cursor.Fetch(&result)
	if err != nil {
		t.Fatalf("unable to fetch: %s", err.Error())
	} else {
		fmt.Println("Record count: ", cursor.Count())
		fmt.Println("Data ", 1, ":", toolkit.JsonString(result))
	}
}

func TestUpdate(t *testing.T) {
	err := sess.NewQuery().From("tabletest").Save().Execute(toolkit.M{}.Set("data", &datamodel{"data04", "Name data04 - New", "Regular"}))
	if err != nil {
		t.Fatalf(err.Error())
	}
}

/*
func TestFetchs(t *testing.T) {
	query = sess.NewQuery()
	query.From("tabletest").Select().Where(dbflex.And(dbflex.Eq("class", 3), dbflex.Gte("yearexperience", 3)))
	cursor = query.Cursor(nil)

	result := []toolkit.M{}
	err = cursor.Fetchs(&result, 0)
	if err != nil {
		t.Fatalf("unable to fetch: %s", err.Error())
	}
}

func TestCrud(t *testing.T) {
	dm := new(datamodel)
	dm.ID = "EMP01"
	dm.Name = "Employee 01"
	dm.Level = "Manager"

	query.Reset().From("employees").Insert()
	err = query.Execute(toolkit.M{}.Set("data", dm))
	check(t, err, "unable to insert")
	dr := Get("EMP01")
	compare(t, dm.Name, dr.Name)

	query.Reset().From("employees").Where(dbflex.Eq("id", "EMP01")).Update()
	dm.Name = "Employee 01 - Update"
	err = query.Execute(toolkit.M{}.Set("data", dm))
	check(t, err, "unable to update")
	dr = Get("EMP01")
	compare(t, dm.Name, dr.Name)

	query.Reset().From("employees").Where(dbflex.Eq("id", "EMP01")).Save()
	dm.Name = "Employee 01 - Saved"
	err = query.Execute(toolkit.M{}.Set("data", dm))
	check(t, err, "unable to save")
	dr = Get("EMP01")
	compare(t, dm.Name, dr.Name)
}
*/

func Get(id string) *datamodel {
	cursor = sess.NewQuery().From("employees").Where(dbflex.Eq("id", id)).Cursor(nil)
	res := &datamodel{}
	if err := cursor.Fetch(&res); err != nil {
		res.Name = ""
	}
	return res
}

func check(t *testing.T, e error, fmt string) {
	if e != nil {
		if fmt == "" {
			fmt = "error: %s"
		}
		t.Fatalf(fmt, e.Error())
	}
}

func compare(t *testing.T, want string, get string) {
	if want != get {
		t.Fatalf("Want %s got %s", want, get)
	}
}

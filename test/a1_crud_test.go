package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/eaciit/dbflex"
	_ "github.com/eaciit/dbflex/drivers/mongodb"
	"github.com/eaciit/toolkit"
)

var (
	conn   dbflex.IConnection
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
		t.Errorf("unable to connect to database. test will be stopped")
		os.Exit(100)
		//t.Fatalf("unable to connect: %s", err.Error())
	}
}

func TestObjectNames(t *testing.T) {
	objnames := conn.ObjectNames(dbflex.ObjTypeAll)
	fmt.Printf("Object retrieves: %d\n", len(objnames))
	for _, v := range objnames {
		fmt.Printf("%v\n", v)
	}
}

func TestFetch(t *testing.T) {
	query = conn.NewQuery()
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
	query = conn.NewQuery()
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
	_, err := conn.NewQuery().From("tabletest").Save().Execute(toolkit.M{}.Set("data", &datamodel{"data04", "Name data04 - New", "Regular"}))
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func Get(id string) *datamodel {
	cursor = conn.NewQuery().From("employees").Where(dbflex.Eq("id", id)).Cursor(nil)
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

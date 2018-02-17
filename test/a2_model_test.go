package test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eaciit/toolkit"

	"github.com/eaciit/dbflex"
)

type model2 struct {
	dbflex.DatamodelBase `bson:"-" json:"-"`
	ID                   string `bson:"_id" json:"_id"`
	Name                 string
	Manager              string
	JoinDate             time.Time
}

func (m *model2) TableName() string {
	return "model2"
}

func (m *model2) Id() (field []string, value []interface{}) {
	return []string{"_id"}, []interface{}{m.ID}
}

func (m *model2) SetId(o []interface{}) {
	m.ID = o[0].(string)
}

var dmtest *model2

func TestModelSave(t *testing.T) {
	dmtest = new(model2)
	dmtest.ID = "testdata"
	dmtest.Name = toolkit.RandomString(32)

	err := dbflex.Save(conn, dmtest)
	check(t, err, "unable to save: %s")
}

func TestModelGet(t *testing.T) {
	dm := new(model2)
	dm.ID = "testdata"
	err := dbflex.Get(conn, dm)
	check(t, err, "unable to get data: %s")

	compare(t, dmtest.Name, dm.Name)
}

func TestModelGets(t *testing.T) {
	var dms []model2
	err := dbflex.Gets(conn, new(model2), &dms, dbflex.NewQueryParam())
	check(t, err, "unable to get data: %s")

	if len(dms) == 0 {
		check(t, errors.New("retrieve nothing"), "unable to get data: %s")
	} else {
		fmt.Printf("Data:\n%s\n", toolkit.JsonString(dms))
	}
}

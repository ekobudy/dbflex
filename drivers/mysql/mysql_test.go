package mysql

import (
	"testing"

	"github.com/eaciit/dbflex"
	"github.com/eaciit/dbflex/testbase"
)

const (
	sqlconnectionstring = "mysql://root:Database.1@localhost:3306/ectestdb"
)

func TestCRUD(t *testing.T) {
	crud := testbase.NewCRUD(t, sqlconnectionstring, 1000, nil)
	crud.Set("deletefilter", dbflex.Eq("id", "EMP-10"))
	crud.RunTest()
}

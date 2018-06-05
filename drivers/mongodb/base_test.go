package mongodb

import (
	"testing"

	"github.com/eaciit/dbflex/testbase"
)

func TestCRUD(t *testing.T) {
	crud := testbase.NewCRUD(t, "mongodb://localhost:27123/dbtest", 10000, nil)
	crud.RunTest()
}

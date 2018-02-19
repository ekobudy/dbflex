package mysql

import "github.com/eaciit/dbflex/drivers/rdbms"

// Cursor represent cursor object. Inherits Cursor object of rdbms drivers and implementation of dbflex.ICursor
type Cursor struct {
	rdbms.Cursor
}

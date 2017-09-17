package mongodb

import (
	"github.com/eaciit/dbflex"
	mgo "gopkg.in/mgo.v2"
)

type Session struct {
	mgosession *mgo.Session
	mgodb      *mgo.Database
}

func (s *Session) Close() {
	if s.mgosession != nil {
		s.mgosession.Close()
	}
}

func (s *Session) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.session = s
	return q
}

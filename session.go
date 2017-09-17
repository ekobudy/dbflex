package dbflex

type ISession interface {
	NewQuery() IQuery
	Close()
}

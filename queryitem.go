package dbflex

const (
	QuerySelect    string = "SELECT"
	QueryFrom             = "FROM"
	QueryWhere            = "WHERE"
	QueryGroup            = "GROUPBY"
	QueryOrder            = "ORDERBY"
	QueryInsert           = "INSERT"
	QueryUpdate           = "UPDATE"
	QueryDelete           = "DELETE"
	QuerySave             = "SAVE"
	QueryCommand          = "COMMAND"
	QueryAggr             = "AGGR"
	QueryCustom           = "CUSTOM"
	QueryTake             = "TAKE"
	QuerySkip             = "SKIP"
	QueryJoin             = "JOIN"
	QueryLeftJoin         = "LEFTJOIN"
	QueryRightJoin        = "RIGHTJOIN"
	QuerySQL              = "SQL"
)

type QueryItem struct {
	Op    string
	Value interface{}
}

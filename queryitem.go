package dbflex

type QueryOp string

const (
	QuerySelect    QueryOp = "SELECT"
	QueryFrom              = "FROM"
	QueryWhere             = "WHERE"
	QueryGroup             = "GROUP BY"
	QueryOrder             = "ORDER BY"
	QueryInsert            = "INSERT"
	QueryUpdate            = "UPDATE"
	QueryDelete            = "DELETE"
	QuerySave              = "SAVE"
	QueryCommand           = "COMMAND"
	QueryAggr              = "AGGR"
	QueryCustom            = "CUSTOM"
	QueryTake              = "TAKE"
	QuerySkip              = "SKIP"
	QueryJoin              = "JOIN"
	QueryLeftJoin          = "LEFT JOIN"
	QueryRightJoin         = "RIGHT JOIN"
	QueryData              = "DATA"
	QueryParm              = "PARM"
	QuerySQL               = "SQL"
)

type QueryItem struct {
	Op    QueryOp
	Value interface{}
}

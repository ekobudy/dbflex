package dbflex

type QueryOp string

const (
	QuerySelect    QueryOp = "SELECT"
	QueryFrom              = "FROM"
	QueryWhere             = "WHERE"
	QueryGroup             = "GROUPBY"
	QueryOrder             = "ORDERBY"
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
	QueryLeftJoin          = "LEFTJOIN"
	QueryRightJoin         = "RIGHTJOIN"
	//QueryData              = "DATA"
	//QueryParm              = "PARM"
	QuerySQL = "SQL"
)

type QueryItem struct {
	Op    QueryOp
	Value interface{}
}

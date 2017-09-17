package dbflex

type AggrOp string

const (
	AggrSum   AggrOp = "$sum"
	AggrAvg          = "$avg"
	AggrMin          = "$min"
	AggrMax          = "$max"
	AggrCount        = "$count"
)

type AggrItem struct {
	Field string
	Op    AggrOp
	Alias string
}

func NewAggrItem(alias string, op AggrOp, field string) *AggrItem {
	a := new(AggrItem)
	if alias == "" {
		alias = field
	}
	a.Alias = alias
	a.Field = field
	a.Op = op
	return a
}

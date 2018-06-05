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

func (a *AggrItem) SetAlias(alias string) {
	a.Alias = alias
}

func Sum(field string) *AggrItem {
	return NewAggrItem(field, AggrSum, field)
}

func Avg(field string) *AggrItem {
	return NewAggrItem(field, AggrAvg, field)
}

func Min(field string) *AggrItem {
	return NewAggrItem(field, AggrMin, field)
}

func Max(field string) *AggrItem {
	return NewAggrItem(field, AggrMax, field)
}

func Count(field string) *AggrItem {
	return NewAggrItem(field, AggrCount, field)
}

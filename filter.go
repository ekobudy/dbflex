package dbflex

type FilterOp string

const (
	OpAnd       FilterOp = "$and"
	OpOr                 = "$or"
	OpEq                 = "$eq"
	OpNe                 = "$ne"
	OpGte                = "$gte"
	OpGt                 = "$gt"
	OpLt                 = "$lt"
	OpLte                = "$lte"
	OpRange              = "$range"
	OpContains           = "$contains"
	OpStartWith          = "$startwith"
	OpEndWith            = "$endwith"
	OpIn                 = "$in"
	OpNin                = "$nin"
)

type Filter struct {
	Items []*Filter
	Field string
	Op    FilterOp
	Value interface{}
}

func NewFilter(field string, op FilterOp, v interface{}, items []*Filter) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = op
	f.Value = v
	if items != nil {
		f.Items = items
	}
	return f
}

func And(items ...*Filter) *Filter {
	return NewFilter("", OpAnd, nil, items)
}

func Or(items ...*Filter) *Filter {
	return NewFilter("", OpOr, nil, items)
}

func Eq(field string, v interface{}) *Filter {
	return NewFilter(field, OpEq, nil, nil)
}

func Ne(field string, v interface{}) *Filter {
	return NewFilter(field, OpNe, v, nil)
}

func Gte(field string, v interface{}) *Filter {
	return NewFilter(field, OpGte, v, nil)
}

func Gt(field string, v interface{}) *Filter {
	return NewFilter(field, OpGt, v, nil)
}

func Lt(field string, v interface{}) *Filter {
	return NewFilter(field, OpLt, v, nil)
}

func Lte(field string, v interface{}) *Filter {
	return NewFilter(field, OpLte, v, nil)
}

func Range(field string, from, to interface{}) *Filter {
	f := NewFilter(field, OpRange, nil, nil)
	f.Value = []interface{}{from, to}
	return f
}

func In(field string, invalues ...interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = OpIn
	f.Value = invalues
	return f
}

func Nin(field string, invalues ...interface{}) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = OpNin
	f.Value = invalues
	return f
}

func Contains(field string, values ...string) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = OpContains
	f.Value = values
	return f
}

func StartWith(field string, values string) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = OpStartWith
	f.Value = values
	return f
}

func EndWith(field string, values string) *Filter {
	f := new(Filter)
	f.Field = field
	f.Op = OpEndWith
	f.Value = values
	return f
}

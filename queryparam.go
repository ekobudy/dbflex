package dbflex

type QueryParam struct {
	Where      *Filter
	Sort       []string
	Take, Skip int
}

func NewQueryParam() *QueryParam {
	return new(QueryParam)
}

func (q *QueryParam) SetWhere(f *Filter) *QueryParam {
	q.Where = f
	return q
}

func (q *QueryParam) SetSort(sorts ...string) *QueryParam {
	q.Sort = sorts
	return q
}

func (q *QueryParam) SetTake(take int) *QueryParam {
	q.Take = take
	return q
}

func (q *QueryParam) SetSkip(skip int) *QueryParam {
	q.Skip = skip
	return q
}

package dbflex

import "github.com/eaciit/toolkit"

type ICommand interface {
	Reset() ICommand
	Select(...string) ICommand
	From(string) ICommand
	Where(*Filter) ICommand
	OrderBy(...string) ICommand
	GroupBy(...string) ICommand

	Aggr(...*AggrItem) ICommand
	Insert(...string) ICommand
	Update(...string) ICommand
	Delete() ICommand
	Save() ICommand

	Take(int) ICommand
	Skip(int) ICommand

	Command(string, interface{}) ICommand
	SQL(string) ICommand
}

type CommandBase struct {
	items []*QueryItem
}

func (b *CommandBase) Reset() ICommand {
	b.items = []*QueryItem{}
	return b
}

func (b *CommandBase) Select(fields ...string) ICommand {
	b.items = append(b.items, &QueryItem{QuerySelect, fields})
	return b
}

func (b *CommandBase) From(name string) ICommand {
	b.items = append(b.items, &QueryItem{QueryFrom, name})
	return b
}

func (b *CommandBase) Where(f *Filter) ICommand {
	b.items = append(b.items, &QueryItem{QueryWhere, f})
	return b
}

func (b *CommandBase) OrderBy(fields ...string) ICommand {
	b.items = append(b.items, &QueryItem{QueryOrder, fields})
	return b
}

func (b *CommandBase) GroupBy(fields ...string) ICommand {
	b.items = append(b.items, &QueryItem{QueryGroup, fields})
	return b
}

func (b *CommandBase) Aggr(aggritems ...*AggrItem) ICommand {
	b.items = append(b.items, &QueryItem{QueryAggr, aggritems})
	return b
}

func (b *CommandBase) Insert(fields ...string) ICommand {
	b.items = append(b.items, &QueryItem{QueryInsert, fields})
	return b
}

func (b *CommandBase) Update(fields ...string) ICommand {
	b.items = append(b.items, &QueryItem{QueryUpdate, fields})
	return b
}

func (b *CommandBase) Delete() ICommand {
	b.items = append(b.items, &QueryItem{QueryDelete, true})
	return b
}

func (b *CommandBase) Save() ICommand {
	b.items = append(b.items, &QueryItem{QuerySave, true})
	return b
}

func (b *CommandBase) Take(n int) ICommand {
	b.items = append(b.items, &QueryItem{QueryTake, n})
	return b
}

func (b *CommandBase) Skip(n int) ICommand {
	b.items = append(b.items, &QueryItem{QuerySkip, n})
	return b
}

func (b *CommandBase) Command(command string, data interface{}) ICommand {
	b.items = []*QueryItem{&QueryItem{QueryCommand, toolkit.M{}.Set("command", command).Set("data", data)}}
	return b
}

func (b *CommandBase) SQL(sql string) ICommand {
	b.items = []*QueryItem{&QueryItem{QuerySQL, sql}}
	return b
}

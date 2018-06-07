package dbflex

import (
	"errors"

	"github.com/eaciit/toolkit"
)

type ICursor interface {
	Reset() error
	Fetch(interface{}) error
	Fetchs(interface{}, int) error
	Count() int
	CountAsync() <-chan int
	Close()
	Error() error
	SetCloseAfterFetch() ICursor
	CloseAfterFetch() bool
	SetCountCommand(ICommand)
	CountCommand() ICommand

	Connection() IConnection
	SetConnection(IConnection)

	ConfigRef(key string, def interface{}, out interface{})
	Set(key string, value interface{})
}

type CursorBase struct {
	err             error
	closeafterfetch bool

	self         ICursor
	countCommand ICommand
	conn         IConnection

	config toolkit.M
}

func (b *CursorBase) SetError(err error) {
	b.err = err
}

func (b *CursorBase) Error() error {
	return b.err
}

func (b *CursorBase) Connection() IConnection {
	return b.conn
}

func (b *CursorBase) ConfigRef(key string, def, out interface{}) {
	if b.config == nil {
		b.config = toolkit.M{}
	}
	b.config.GetRef(key, def, out)
}

func (b *CursorBase) Set(key string, value interface{}) {
	if b.config == nil {
		b.config = toolkit.M{}
	}
	b.config.Set(key, value)
}

func (b *CursorBase) SetConnection(conn IConnection) {
	b.conn = conn
}

func (b *CursorBase) this() ICursor {
	if b.self == nil {
		return b
	} else {
		return b.self
	}
}

func (b *CursorBase) SetThis(o ICursor) ICursor {
	b.self = o
	return o
}

func (b *CursorBase) Reset() error {
	return errors.New("not implemented")
}

func (b *CursorBase) Fetch(interface{}) error {
	return errors.New("not implemented")
}

func (b *CursorBase) Fetchs(interface{}, int) error {
	return errors.New("not implemented")
}

func (b *CursorBase) Count() int {
	if b.countCommand == nil {
		b.SetError(toolkit.Errorf("cursor has no count command"))
		return 0
	}

	recordcount := struct {
		Count int
	}{}

	if b.conn == nil {
		b.SetError(toolkit.Errorf("connection object is not defined"))
		return 0
	}

	//err := b.countCommand.Cursor(nil).Fetch(&recordcount)
	err := b.conn.Cursor(b.CountCommand(), nil).Fetch(&recordcount)
	if err != nil {
		b.SetError(toolkit.Errorf("unable to get count. %s", err.Error()))
		return 0
	}

	return recordcount.Count
}

func (b *CursorBase) CountAsync() <-chan int {
	out := make(chan int)
	go func(o chan int) {
		o <- b.Count()
	}(out)
	return out
}

func (b *CursorBase) SetCountCommand(q ICommand) {
	b.countCommand = q
}

func (b *CursorBase) CountCommand() ICommand {
	return b.countCommand
}

func (b *CursorBase) SetCloseAfterFetch() ICursor {
	b.closeafterfetch = true
	return b.this()
}

func (b *CursorBase) CloseAfterFetch() bool {
	return b.closeafterfetch
}

func (b *CursorBase) Close() {
}

func (b *CursorBase) Serialize(dest interface{}) error {
	return toolkit.Error("Serialize is not yet implemented")
}

package dbflex

import (
	"github.com/eaciit/toolkit"
)

type ICursor interface {
	Reset() error
	Fetch(interface{}) error
	Fetchs(interface{}, int) error
	Count() int
	Close()
	Error() error
	SetCloseAfterFetch() ICursor
	CloseAfterFetch() bool
}

type CursorBase struct {
	err             error
	closeafterfetch bool

	self ICursor
}

func (b *CursorBase) SetError(err error) {
	b.err = err
}

func (b *CursorBase) Error() error {
	return b.err
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
	panic("not implemented")
}

func (b *CursorBase) Fetch(interface{}) error {
	panic("not implemented")
}

func (b *CursorBase) Fetchs(interface{}, int) error {
	panic("not implemented")
}

func (b *CursorBase) Count() int {
	return 0
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

package dbflex

import (
	"net/url"

	"github.com/eaciit/toolkit"
)

var drivers map[string]func(*ServerInfo) IConnection

type ObjTypeEnum string

const (
	ObjTypeTable     ObjTypeEnum = "table"
	ObjTypeView      ObjTypeEnum = "view"
	ObjTypeProcedure ObjTypeEnum = "procedure"
	ObjTypeAll       ObjTypeEnum = "allobject"

	StateConnected string = "connected"
	StateUnknown          = ""
)

// IConnection provides interface for database connection
type IConnection interface {
	Connect() error
	State() string
	Close()

	Prepare(ICommand) (IQuery, error)
	Execute(ICommand, toolkit.M) (interface{}, error)
	Cursor(ICommand, toolkit.M) ICursor

	NewQuery() IQuery
	ObjectNames(ObjTypeEnum) []string
	ValidateTable(interface{}, bool) error
	DropTable(string) error

	SetThis(IConnection) IConnection
	This() IConnection

	SetFieldNameTag(string)
	FieldNameTag() string
}

// ConnectionBase is base class to implement IConnection interface
type ConnectionBase struct {
	ServerInfo

	_this IConnection

	fieldnameTag string
}

func (b *ConnectionBase) SetThis(t IConnection) IConnection {
	b._this = t
	return t
}

func (b *ConnectionBase) This() IConnection {
	if b._this == nil {
		return b
	} else {
		return b._this
	}
}

func (b *ConnectionBase) SetFieldNameTag(name string) {
	b.fieldnameTag = name
}

func (b *ConnectionBase) FieldNameTag() string {
	return b.fieldnameTag
}

// Connect establish connection
func (b *ConnectionBase) Connect() error {
	return toolkit.Error("Connect method is not yet implemented. It should be called from a driver connection object")
}
func (b *ConnectionBase) State() string { return StateUnknown }
func (b *ConnectionBase) Close()        {}
func (b *ConnectionBase) NewQuery() IQuery {
	return nil
}

func (b *ConnectionBase) ObjectNames(ot ObjTypeEnum) []string {
	return []string{}
}

func (b *ConnectionBase) ValidateTable(obj interface{}, autoUpdate bool) error {
	return toolkit.Errorf("ValidateSchema is not yet implemented")
}

func (b *ConnectionBase) DropTable(name string) error {
	return toolkit.Errorf("DropTable is not yet implemented")
}

func (b *ConnectionBase) Prepare(cmd ICommand) (IQuery, error) {
	var dbCmd interface{}

	if b.This().State() != StateConnected {
		return nil, toolkit.Errorf("no valid connection")
	}

	q := b.This().NewQuery()
	err := buildGroupedQueryItems(cmd, q)
	if err == nil {
		dbCmd, err = q.This().BuildCommand()
	}

	if err != nil {
		return nil, toolkit.Errorf("unable to parse command. %s", err)
	}
	q.SetConfig(ConfigKeyCommand, dbCmd)
	return q, nil
}

func (b *ConnectionBase) Execute(c ICommand, m toolkit.M) (interface{}, error) {
	q, err := b.Prepare(c)
	if err != nil {
		return nil, toolkit.Errorf("unable to prepare query. %s", err.Error())
	}
	q.SetConnection(b.This())
	return q.Execute(m)
}

func (b *ConnectionBase) Cursor(c ICommand, m toolkit.M) ICursor {
	q, err := b.Prepare(c)
	if err != nil {
		//return nil, toolkit.Errorf("usnable to prepare query. %s", err.Error())
		cursor := new(CursorBase)
		cursor.SetError(toolkit.Errorf("unable to prepare query. %s", err.Error()))
		return cursor
	}
	cursor := q.Cursor(m)
	cursor.SetConnection(b.This())
	return cursor
}

type ServerInfo struct {
	Host, User, Password, Database string
	Config                         toolkit.M
}

func RegisterDriver(name string, fn func(*ServerInfo) IConnection) {
	if drivers == nil {
		drivers = map[string]func(*ServerInfo) IConnection{}
	}
	drivers[name] = fn
}

func NewConnectionFromConfig(driver, path, name string) IConnection {
	/*
		if fn, ok := drivers[driver]; ok{
			return fn()
		}
	*/
	return nil
}

func NewConnectionFromUri(uri string, config toolkit.M) (IConnection, error) {
	u, e := url.Parse(uri)
	if e != nil {
		return nil, e
	}

	driver := u.Scheme
	if fn, ok := drivers[driver]; ok {
		si := new(ServerInfo)
		si.Host = u.Host
		if len(u.Path) > 1 {
			si.Database = u.Path[1:]
		}
		if config != nil {
			si.Config = config
		} else {
			si.Config = toolkit.M{}
		}
		if u.RawQuery != "" {
			mq, e := url.ParseQuery(u.RawQuery)
			if e == nil {
				for k, v := range mq {
					si.Config.Set(k, v)
				}
			}
		}
		if u.User != nil {
			si.User = u.User.Username()
			si.Password, _ = u.User.Password()
		}
		return fn(si), nil
	}
	return nil, toolkit.Errorf("driver %s is unknown", driver)
}

func From(tableName string) ICommand {
	return new(CommandBase).From(tableName)
}

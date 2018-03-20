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
)

// IConnection provides interface for database connection
type IConnection interface {
	Connect() error
	State() string
	Close()
	NewQuery() IQuery
	ObjectNames(ObjTypeEnum) []string
}

// ConnectionBase is base class to implement IConnection interface
type ConnectionBase struct {
	ServerInfo
}

// Connect establish connection
func (b *ConnectionBase) Connect() error {
	return toolkit.Error("Connect method is not yet implemented. It should be called from a driver connection object")
}
func (b *ConnectionBase) State() string { return "" }
func (b *ConnectionBase) Close()        {}
func (b *ConnectionBase) NewQuery() IQuery {
	return nil
}
func (b *ConnectionBase) ObjectNames(ot ObjTypeEnum) []string {
	return []string{}
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

func NewConnectionFromUri(uri string, config toolkit.M) IConnection {
	u, e := url.Parse(uri)
	if e != nil {
		return nil
	}

	driver := u.Scheme
	if fn, ok := drivers[driver]; ok {
		si := new(ServerInfo)
		si.Host = u.Host
		if config != nil {
			si.Config = config
		} else {
			si.Config = toolkit.M{}
		}
		if u.RawQuery != "" {
			mq, e := url.ParseQuery(u.RawQuery)
			if e != nil {
				for k, v := range mq {
					si.Config.Set(k, v)
				}
			}
		}
		return fn(si)
	}
	return nil
}

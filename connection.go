package dbflex

import (
	"strings"

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
	Port                           int
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

func NewConnectionFromUri(driver, uri string, config toolkit.M) IConnection {
	if fn, ok := drivers[driver]; ok {
		si := ParseServerInfo(uri)
		si.Config = config
		return fn(si)
	}
	return nil
}

func ParseServerInfo(s string) *ServerInfo {
	logins := ""
	hosts := ""
	loginhosts := strings.Split(s, "@")

	if len(loginhosts) >= 2 {
		logins = loginhosts[0]
		hosts = loginhosts[1]
	} else if len(loginhosts) == 1 {
		hosts = loginhosts[0]
	}

	si := new(ServerInfo)
	if logins != "" {
		details := strings.Split(logins, ":")
		if len(details) >= 2 {
			si.User = details[0]
			si.Password = details[1]
		} else if len(details) == 1 {
			si.User = details[0]
		}
	}

	if hosts != "" {
		details := strings.Split(hosts, "/")
		if len(details) == 2 {
			si.Database = details[1]
			hosts = details[0]

		}

		details = strings.Split(hosts, ":")
		if len(details) == 2 {
			si.Port = toolkit.ToInt(details[1], toolkit.RoundingAuto)
			si.Host = details[0]
		} else if len(details) == 1 {
			si.Host = details[0]
		}
	}
	return si
}

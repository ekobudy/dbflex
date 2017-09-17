package dbflex


type IDataModel interface {
	TableName() string
	IDFields() []string
}
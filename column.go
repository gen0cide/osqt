package osqt

import (
	"golang.org/x/xerrors"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Column represents a column definition within an OSQuery table declaration.
type Column struct {
	Index       int                    `json:"index" yaml:"index"`
	Name        string                 `json:"name,omitempty" yaml:"name,omitempty"`
	Type        string                 `json:"type,omitempty" yaml:"type,omitempty"`
	Description string                 `json:"description,omitempty" yaml:"description,omitempty"`
	Aliases     []string               `json:"aliases,omitempty" yaml:"aliases,omitempty"`
	Options     map[string]interface{} `json:"options,omitempty" yaml:"options,omitempty"`
}

// NewEmptyColumn creates a new empty Column object.
func NewEmptyColumn() *Column {
	return &Column{
		Aliases: []string{},
		Options: map[string]interface{}{},
	}
}

// ToSQLSchema creates a virtual sql.Column definition to be used in construction of the virtual database.
func (c *Column) ToSQLSchema(tablename string) *sql.Column {
	col := &sql.Column{}
	col.Name = c.Name
	col.Source = tablename
	col.Nullable = true

	switch c.Type {
	case "TEXT":
		col.Type = sql.Text
	case "DATE":
		col.Type = sql.Date
	case "DATETIME":
		col.Type = sql.Timestamp
	case "INTEGER":
		col.Type = sql.Int32
	case "BIGINT":
		col.Type = sql.Int64
	case "UNSIGNED_BIGINT":
		col.Type = sql.Uint64
	case "DOUBLE":
		col.Type = sql.Float64
	case "BLOB":
		col.Type = sql.Blob
	default:
		err := xerrors.Errorf("unsupported type %s for column %s", c.Type, c.Name)
		panic(err)
	}

	return col
}

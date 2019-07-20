package osqt

import (
	"fmt"
	"sync"

	past "github.com/go-python/gpython/ast"
	"github.com/k0kubun/pp"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Table represents an OSQuery table and all of it's properties and schema.
type Table struct {
	sync.RWMutex

	logger *zap.SugaredLogger

	Namespace       *Namespace             `json:"-" yaml:"-"`
	NamespaceID     string                 `json:"namespace_id,omitempty" yaml:"namespace_id,omitempty"`
	Name            string                 `json:"name,omitempty" yaml:"name,omitempty"`
	Aliases         []string               `json:"aliases,omitempty" yaml:"aliases,omitempty"`
	Description     string                 `json:"description,omitempty" yaml:"description,omitempty"`
	Schema          *Schema                `json:"schema,omitempty" yaml:"schema,omitempty"`
	Attributes      map[string]interface{} `json:"attributes,omitempty" yaml:"attributes,omitempty"`
	Implementation  string                 `json:"implementation,omitempty" yaml:"implementation,omitempty"`
	FuzzPaths       []string               `json:"fuzz_paths,omitempty" yaml:"fuzz_paths,omitempty"`
	ExtendedSchemas map[string]*Schema     `json:"extended_schemas,omitempty" yaml:"extended_schemas,omitempty"`
	Examples        []string               `json:"examples,omitempty" yaml:"examples,omitempty"`
}

// Logger returns or creates a new table logger
func (t *Table) Logger() *zap.SugaredLogger {
	if t.logger == nil {
		t.logger = zap.L().Sugar().Named(t.Name)
	}

	return t.logger
}

// NewEmptyTable is a constructor for the Table type.
func NewEmptyTable() *Table {
	return &Table{
		Aliases:         []string{},
		Attributes:      map[string]interface{}{},
		FuzzPaths:       []string{},
		ExtendedSchemas: map[string]*Schema{},
		Examples:        []string{},
	}
}

// Visit is the AST walk implementation for the Python interpreter.
func (t *Table) Visit(pyast past.Ast) bool {
	switch node := pyast.(type) {
	case *past.Call:
		return t.VisitorBranch(node)
	default:
		return true
	}
}

// VisitorBranch attempts to route the node extraction based on the caller function name.
func (t *Table) VisitorBranch(node *past.Call) bool {
	funcToken, ok := node.Func.(*past.Name)
	if !ok {
		astNode := pp.Sprint(node)
		t.Logger().Debugf("Failed AST Node: \n%s\n", astNode)
		t.Logger().Error("AST function node invalid")
		return false
	}

	funcName := string(funcToken.Id)
	switch funcName {
	case "table_name":
		err := t.ExtractNames(node)
		if err != nil {
			panic(err)
		}
	case "description":
		err := t.ExtractDescription(node)
		if err != nil {
			panic(err)
		}
	case "schema":
		err := t.ExtractSchema(node)
		if err != nil {
			panic(err)
		}
	case "Column":
		return false
	case "attributes":
		t.ExtractAttributes(node)
		return false
	case "implementation":
		err := t.ExtractImplementation(node)
		if err != nil {
			panic(err)
		}
	case "fuzz_paths":
		err := t.ExtractFuzzPaths(node)
		if err != nil {
			panic(err)
		}
		return false
	case "extended_schema":
		err := t.ExtractExtendedSchema(node)
		if err != nil {
			panic(err)
		}
		return false
	case "examples":
		err := t.ExtractExamples(node)
		if err != nil {
			panic(err)
		}
		return false
	case "ForeignKey":
		return false
	default:
		astNode := pp.Sprint(node)
		t.Logger().Debugf("AST Node: \n\n%s", astNode)
		t.Logger().Errorw("Unhandled AST caller", "function", funcName)
		return false
	}

	return true
}

// ExtractFuzzPaths attempts to extract the fuzz_paths([]) declaration for compiler checking.
func (t *Table) ExtractFuzzPaths(node *past.Call) error {
	arglist, ok := node.Args[0].(*past.List)
	if !ok {
		err := xerrors.New("argument 0 was not of type *arg.List")
		t.Logger().Errorw("spec parsing error", "error", err)
		return err
	}
	for elmidx, def := range arglist.Elts {
		strval, ok := def.(*past.Str)
		if !ok {
			err := xerrors.Errorf("expected *past.Str field, got %T for argument list element %d", def, elmidx)
			t.Logger().Errorw("spec parsing error", "error", err)
			return err
		}
		t.FuzzPaths = append(t.FuzzPaths, string(strval.S))
	}

	t.Logger().Debug("Extracted table fuzz_paths")
	return nil
}

// ExtractExamples attempts to extract the examples([]) delaration of example queries.
func (t *Table) ExtractExamples(node *past.Call) error {
	arglist, ok := node.Args[0].(*past.List)
	if !ok {
		err := xerrors.New("argument 0 was not of type *arg.List")
		t.Logger().Errorw("spec parsing error", "error", err)
		return err
	}
	for elmidx, def := range arglist.Elts {
		strval, ok := def.(*past.Str)
		if !ok {
			err := xerrors.Errorf("expected *past.Str field, got %T for argument list element %d", def, elmidx)
			t.Logger().Errorw("spec parsing error", "error", err)
			return err
		}
		t.Examples = append(t.Examples, string(strval.S))
	}
	t.Logger().Debug("Extracted table examples")
	return nil
}

// ExtractAttributes attempts to extract the attributes() declaration of keyword arguments.
func (t *Table) ExtractAttributes(node *past.Call) {
	if t.Attributes == nil {
		t.Attributes = map[string]interface{}{}
	}
	for _, kw := range node.Keywords {
		optkey := string(kw.Arg)
		switch v := kw.Value.(type) {
		case *past.NameConstant:
			t.Attributes[optkey] = v.Value
		case *past.Str:
			t.Attributes[optkey] = string(v.S)
		case *past.Name:
			t.Attributes[optkey] = string(v.Id)
		}
	}
	t.Logger().Debug("Extracted table attributes")
}

// ExtractSchema attempts to extract the primary schema for an OSQuery table's schema([]) declaration.
func (t *Table) ExtractSchema(node *past.Call) error {
	if t.Schema != nil {
		err := xerrors.New("schema is already a non-nil value for table")
		t.Logger().Errorw("spec parsing error", "error", err)
		return err
	}
	t.Schema = NewEmptySchema(t)

	t.Logger().Debug("Extracted table schema")
	return t.Schema.ExtractSchema(node)

}

// ExtractExtendedSchema attempts to extract the platform-dependant schemas from an OSQuery table's extended_schema(PLATFORM, []) declaration.
func (t *Table) ExtractExtendedSchema(node *past.Call) error {
	extSchema := NewEmptySchema(t)

	err := extSchema.ExtractSchema(node)
	if err != nil {
		return err
	}

	t.Lock()
	defer t.Unlock()
	for _, platform := range extSchema.Platforms {
		t.ExtendedSchemas[platform] = extSchema
		t.Logger().Debugf("Extracted extended_schema for %s", platform)
	}

	return nil
}

// ExtractImplementation attempts to extract the table implementation("...") declaration.
func (t *Table) ExtractImplementation(node *past.Call) error {
	impl, ok := node.Args[0].(*past.Str)
	if !ok {
		return fmt.Errorf("argument 0 was not of type string")
	}
	t.Implementation = string(impl.S)
	t.Logger().Debug("Extracted table implementation")

	return nil
}

// ExtractDescription attempts to extract the table description("...") declaration.
func (t *Table) ExtractDescription(node *past.Call) error {
	desc, ok := node.Args[0].(*past.Str)
	if !ok {
		return fmt.Errorf("argument 0 was not of type string")
	}
	t.Description = string(desc.S)
	t.Logger().Debug("Extracted table description")

	return nil
}

// ExtractNames attempts to parse the table_name("foo") declaration.
func (t *Table) ExtractNames(node *past.Call) error {
	tblname, ok := node.Args[0].(*past.Str)
	if !ok {
		return fmt.Errorf("argument 0 was not of type string")
	}
	t.Name = string(tblname.S)

	if len(node.Keywords) > 0 {
		for _, kw := range node.Keywords {
			if string(kw.Arg) != "aliases" {
				fmt.Printf("[!] Unhandled Keyword Argument: %s (%s)\n", string(kw.Arg), t.Name)
				continue
			}
			aliasList, ok := kw.Value.(*past.List)
			if !ok {
				fmt.Printf("[!] aliases keyword argument is not of type *ast.List! (%s)\n", t.Name)
				continue
			}
			for idx, elm := range aliasList.Elts {
				aliasName, ok := elm.(*past.Str)
				if !ok {
					fmt.Printf("[!] aliases keyword argument index %d is not of type *ast.Str! (%s)\n", idx, t.Name)
					continue
				}
				t.Aliases = append(t.Aliases, string(aliasName.S))
			}
		}
	}
	t.Logger().Debug("Extracted table name and alias")
	return nil
}

func (t *Table) ToSQLSchema(extendedSchemas []string) sql.Schema {
	cols := []*sql.Column{}
	for _, col := range t.Schema.Columns {
		cols = append(cols, col.ToSQLSchema(t.Name))
	}

	for _, ext := range extendedSchemas {
		extschema, found := t.ExtendedSchemas[ext]
		if !found {
			continue
		}
		for _, col := range extschema.Columns {
			cols = append(cols, col.ToSQLSchema(t.Name))
		}
	}

	return cols
}

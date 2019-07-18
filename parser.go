package osqt

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	past "github.com/go-python/gpython/ast"
	gparser "github.com/go-python/gpython/parser"
	"github.com/k0kubun/pp"
	"github.com/karrick/godirwalk"
)

// Parser is a directory walking extraction of OSQuery table definitions. (usually specs/)
type Parser struct {
	sync.RWMutex

	BaseDir string
	Tables  map[string]*Table `json:"tables,omitempty"`
}

// SourceFile is used to define a file containing an OSQuery table definition.
type SourceFile struct {
	Path  string
	Table *Table
}

// Table represents an OSQuery table and all of it's properties and schema.
type Table struct {
	Name           string                 `json:"name,omitempty" yaml:"name,omitempty"`
	Aliases        []string               `json:"aliases,omitempty" yaml:"aliases,omitempty"`
	Description    string                 `json:"description,omitempty" yaml:"description,omitempty"`
	Schema         []*Column              `json:"schema,omitempty" yaml:"schema,omitempty"`
	Attributes     map[string]interface{} `json:"attributes,omitempty" yaml:"attributes,omitempty"`
	Implementation string                 `json:"implementation,omitempty" yaml:"implementation,omitempty"`
	FuzzPaths      []string               `json:"fuzz_paths,omitempty" yaml:"fuzz_paths,omitempty"`
}

// Column represents a column definition within an OSQuery table declaration.
type Column struct {
	Index       int                    `json:"index" yaml:"index"`
	Name        string                 `json:"name,omitempty" yaml:"name,omitempty"`
	Type        string                 `json:"type,omitempty" yaml:"type,omitempty"`
	Description string                 `json:"description,omitempty" yaml:"description,omitempty"`
	Aliases     []string               `json:"aliases,omitempty" yaml:"aliases,omitempty"`
	Options     map[string]interface{} `json:"options,omitempty" yaml:"options,omitempty"`
}

// NewParser returns a new parser for extracting structured OSQuery
// table definitions from within their .table declaration files.
func NewParser() *Parser {
	return &Parser{
		Tables: map[string]*Table{},
	}
}

// ParseDirectory walks a directory structure for all .table files and attempts to parse
// them as OSQuery table defintiions.
func (p *Parser) ParseDirectory(location string) error {
	errchan := make(chan error, 1)
	reschan := make(chan *SourceFile, 1000)
	finchan := make(chan bool, 1)

	go func() {
		p.Lock()
		defer func() {
			finchan <- true
		}()
		defer p.Unlock()
		for src := range reschan {
			p.Tables[src.Table.Name] = src.Table
		}
	}()

	p.BaseDir = location

	go func() {
		defer close(reschan)
		err := godirwalk.Walk(p.BaseDir, &godirwalk.Options{
			Callback: func(fileloc string, de *godirwalk.Dirent) error {
				if !de.IsRegular() || filepath.Ext(fileloc) != ".table" {
					return nil
				}
				fin, err := os.Open(fileloc)
				if err != nil {
					return err
				}

				tbl, err := ParseTableDef(fin)
				if err != nil {
					pp.Println(fileloc)
					return err
				}

				reschan <- &SourceFile{
					Path:  fileloc,
					Table: tbl,
				}
				return nil
			},
			Unsorted: true,
		})
		if err != nil {
			errchan <- err
		}
	}()

	select {
	case err := <-errchan:
		return err
	case <-finchan:
		return nil
	}
}

// NewEmptyTable is a constructor for the Table type.
func NewEmptyTable() *Table {
	return &Table{
		Aliases:    []string{},
		Schema:     []*Column{},
		Attributes: map[string]interface{}{},
		FuzzPaths:  []string{},
	}
}

// NewEmptyColumn creates a new empty Column object.
func NewEmptyColumn() *Column {
	return &Column{
		Aliases: []string{},
		Options: map[string]interface{}{},
	}
}

// ParseTableDef takes an input of Python source and attempts to extract an OSQuery table
// definition by extracting the information out of the Python AST that is generated
// on the fly.
func ParseTableDef(in io.Reader) (*Table, error) {
	t := &Table{}
	gpyast, err := gparser.Parse(in, "", "exec")
	if err != nil {
		return nil, err
	}

	past.Walk(gpyast, t.Visit)

	return t, nil
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
		pp.Println("Failed AST Node")
		pp.Println(node)
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
		fmt.Printf("[%s] Skipping attributes() Parsing for table\n", t.Name)
		return false
	case "implementation":
		err := t.ExtractImplementation(node)
		if err != nil {
			panic(err)
		}
	case "fuzz_paths":
		fmt.Printf("[%s] Skipping fuzz_path() Parsing for table\n", t.Name)
		return false
	case "extended_schema":
		fmt.Printf("[%s] Skipping extended_schema() Parsing for table\n", t.Name)
		return false
	case "examples":
		fmt.Printf("[%s] Skipping examples() Parsing for table\n", t.Name)
		return false
	case "ForeignKey":
		return false
	default:
		fmt.Printf("[%s] Unhandled Caller: %s\n", t.Name, funcName)
		pp.Println(node)
		return false
	}

	return true
}

// ExtractSchema attempts to extract the schema([]) declaraction.
func (t *Table) ExtractSchema(node *past.Call) error {
	arglist, ok := node.Args[0].(*past.List)
	if !ok {
		return fmt.Errorf("argument 0 was not of type *arg.List (%s)", t.Name)
	}
	for colidx, coldef := range arglist.Elts {
		coldefcaller, ok := coldef.(*past.Call)
		if !ok {
			return fmt.Errorf("argument %d was not of type *ast.Call (%s)", colidx, t.Name)
		}

		col := NewEmptyColumn()
		col.Index = colidx

		if len(coldefcaller.Args) < 1 {
			fmt.Printf("[%s] Non Column() definition detected! Skipping...", t.Name)
			continue
		}

		if nameObj, ok := coldefcaller.Args[0].(*past.Str); ok {
			col.Name = string(nameObj.S)
		}

		if typeObj, ok := coldefcaller.Args[1].(*past.Name); ok {
			col.Type = string(typeObj.Id)
		}

		if descObj, ok := coldefcaller.Args[2].(*past.Str); ok {
			col.Description = string(descObj.S)
		}

		for _, kw := range coldefcaller.Keywords {
			optkey := string(kw.Arg)
			switch v := kw.Value.(type) {
			case *past.NameConstant:
				col.Options[optkey] = v.Value
			case *past.Str:
				col.Options[optkey] = string(v.S)
			case *past.Name:
				col.Options[optkey] = string(v.Id)
			}
		}

		t.Schema = append(t.Schema, col)
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

	return nil
}

// ExtractDescription attempts to extract the table description("...") declaration.
func (t *Table) ExtractDescription(node *past.Call) error {
	desc, ok := node.Args[0].(*past.Str)
	if !ok {
		return fmt.Errorf("argument 0 was not of type string")
	}
	t.Description = string(desc.S)

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
	return nil
}

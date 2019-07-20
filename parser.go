package osqt

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	past "github.com/go-python/gpython/ast"
	gparser "github.com/go-python/gpython/parser"
	"github.com/karrick/godirwalk"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// Parser is a directory walking extraction of OSQuery table definitions. (usually specs/)
type Parser struct {
	sync.RWMutex

	SchemaFile string
	BaseDir    string
	Logger     *zap.SugaredLogger
	Namespaces map[string]*Namespace `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
}

// SourceFile is used to define a file containing an OSQuery table definition.
type SourceFile struct {
	Path  string
	Table *Table
}

// NewParser returns a new parser for extracting structured OSQuery
// table definitions from within their .table declaration files.
func NewParser(logger *zap.SugaredLogger) *Parser {
	if logger == nil {
		logger = zap.L().Sugar().Named("parser")
	}
	return &Parser{
		Logger:     logger,
		Namespaces: map[string]*Namespace{},
	}
}

// ParseYAMLSchemaFile attempts to recreate a table structure from a YAML schema definition.
func (p *Parser) ParseYAMLSchemaFile(fileloc string) error {
	filebytes, err := ioutil.ReadFile(fileloc)
	if err != nil {
		return err
	}

	tables := map[string]*Namespace{}
	err = yaml.Unmarshal(filebytes, &tables)
	if err != nil {
		return err
	}

	return p.InjectTables(tables)
}

// ParseJSONSchemaFile attempts to parse a table structure from a JSON schema definition.
func (p *Parser) ParseJSONSchemaFile(fileloc string) error {
	filebytes, err := ioutil.ReadFile(fileloc)
	if err != nil {
		return err
	}

	tables := map[string]*Namespace{}
	err = json.Unmarshal(filebytes, &tables)
	if err != nil {
		return err
	}

	return p.InjectTables(tables)
}

// InjectTables is used to "wire up" tables and their child types with the current Parser.
func (p *Parser) InjectTables(raw map[string]*Namespace) error {
	for nsid, ns := range raw {
		if ns.parser == nil {
			ns.parser = p
		}
		for tname, table := range ns.Tables {
			table.logger = ns.Logger().Named(tname)
			table.Namespace = ns
			if table.NamespaceID == "" {
				table.NamespaceID = nsid
			}
			if table.Schema != nil {
				table.Schema.logger = table.logger.Named("schema")
				table.Schema.Table = table
			}

			for esname, es := range table.ExtendedSchemas {
				es.logger = table.logger.Named("extended_schema").Named(esname)
				es.Table = table
			}
		}

		p.Namespaces[nsid] = ns
	}

	return nil
}

// ParseDirectory walks a directory structure for all .table files and attempts to parse
// them as OSQuery table defintiions.
func (p *Parser) ParseDirectory(location string) error {
	errchan := make(chan error, 1)
	reschan := make(chan *SourceFile, 1000)
	finchan := make(chan bool, 1)

	go func() {
		p.Logger.Debug("Starting record keeping worker.")
		p.Lock()
		defer func() {
			p.Logger.Debug("Shutting down record keeping worker.")
			finchan <- true
		}()
		defer p.Unlock()
		for src := range reschan {
			namespaceID := filepath.Base(filepath.Dir(src.Path))
			namespaceDescription, ok := CanonicalPlatforms[namespaceID]
			if !ok {
				p.Logger.Fatalw("Could not find namespace", "nsid", namespaceID, "path", src.Path, "dir", filepath.Dir(src.Path), "base", filepath.Base(filepath.Dir(src.Path)))
			}
			p.Logger.Debugw("Table recorded", "table", src.Table.Name, "nsid", namespaceID, "ns", namespaceDescription)
			ns, ok := p.Namespaces[namespaceID]
			if !ok {
				ns = NewNamespace(namespaceID, namespaceDescription, p, nil)
				p.Namespaces[namespaceID] = ns
			}
			src.Table.NamespaceID = namespaceID
			src.Table.Namespace = ns
			ns.Tables[src.Table.Name] = src.Table
		}
	}()

	p.BaseDir = location

	go func() {
		defer close(reschan)
		p.Logger.Debug("Walking base directory.")
		err := godirwalk.Walk(p.BaseDir, &godirwalk.Options{
			Callback: func(fileloc string, de *godirwalk.Dirent) error {
				if !de.IsRegular() || filepath.Ext(fileloc) != ".table" {
					return nil
				}

				tbl, err := p.ParseTableDef(fileloc)
				if err != nil {
					p.Logger.Warnw("Error parsing spec file.", "file", fileloc, "error", err)
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

// ParseTableDef takes an input of Python source and attempts to extract an OSQuery table
// definition by extracting the information out of the Python AST that is generated
// on the fly.
func (p *Parser) ParseTableDef(fileloc string) (*Table, error) {
	freader, err := os.Open(fileloc)
	if err != nil {
		p.Logger.Debugw("Error encountered opening spec file.", "file", fileloc, "error", err)
		return nil, err
	}

	filename := strings.Replace(filepath.Base(fileloc), ".table", "", -1)

	t := NewEmptyTable()
	t.Name = filename
	t.logger = p.Logger.Named(filename)
	gpyast, err := gparser.Parse(freader, filepath.Base(fileloc), "exec")
	if err != nil {
		return nil, err
	}

	past.Walk(gpyast, t.Visit)

	return t, nil
}

package virtual

import (
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"github.com/gen0cide/osqt"
)

// ErrDatabaseInitialized is thrown when a database has already been initialized, and thus is immutable and cannot be modified.
var ErrDatabaseInitialized = xerrors.New("database has already been initialized and cannot be modified")

// Database is a virtual database that can be used to construct a virtual OSQuery engine.
type Database struct {
	sync.RWMutex

	initialized bool
	name        string
	logger      *zap.SugaredLogger
	eng         *sqle.Engine
	instance    *mem.Database
	memtables   map[string]*mem.Table
	schemas     map[string]sql.Schema
	pid         *atomic.Uint64
	parser      *osqt.Parser
}

// NewDatabase creates an uninitialized, base Database object with some basic settings pre-configured.
func NewDatabase(name string, parser *osqt.Parser, logger *zap.SugaredLogger) (*Database, error) {
	if parser == nil {
		return nil, xerrors.New("must provide a parser to construct a database from")
	}

	if name == "" {
		name = "osquery"
	}
	if logger == nil {
		logger = zap.L().Sugar().Named("vdb")
		if name != "" {
			logger = logger.Named(name)
		}
	}

	return &Database{
		name:      name,
		parser:    parser,
		logger:    logger,
		pid:       atomic.NewUint64(uint64(10)),
		memtables: map[string]*mem.Table{},
		schemas:   map[string]sql.Schema{},
	}, nil
}

// AddTable adds table to the Database's schema manifest.
func (d *Database) AddTable(tbl *osqt.Table, osexts []string) error {
	if d.initialized {
		return ErrDatabaseInitialized
	}

	d.Lock()
	defer d.Unlock()

	schema := tbl.ToSQLSchema(osexts)
	d.schemas[tbl.Name] = schema
	return nil
}

// Initialize takes the schemas recorded via AddTable, and initializes a database engine supporting that schema set.
func (d *Database) Initialize() error {
	if d.initialized {
		return ErrDatabaseInitialized
	}

	d.Lock()
	defer d.Unlock()

	db := mem.NewDatabase(d.name)
	for tblname, tblschema := range d.schemas {
		table := mem.NewTable(tblname, tblschema)
		db.AddTable(tblname, table)
		d.memtables[tblname] = table
	}
	eng := sqle.NewDefault()
	eng.AddDatabase(db)
	err := eng.Init()
	if err != nil {
		return xerrors.Errorf("error initializing database: %v", err)
	}

	d.initialized = true
	d.eng = eng
	d.instance = db
	return nil
}

// Start is used to create a listener for the Database and start a server loop to handle sessions. This function will not return unless the server shuts down.
func (d *Database) Start(proto, addr string) error {
	if !d.initialized {
		return xerrors.New("server cannot start until the database is initialized")
	}
	cfg := server.Config{
		Protocol: proto,
		Address:  addr,
		Auth:     &auth.None{},
	}

	svr, err := server.NewDefaultServer(cfg, d.eng)
	if err != nil {
		return err
	}

	err = svr.Start()
	if err != nil {
		return err
	}

	return nil
}

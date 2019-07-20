package main

import (
	"path/filepath"
	"runtime"

	"github.com/urfave/cli"
	"golang.org/x/xerrors"

	"github.com/gen0cide/osqt"
	"github.com/gen0cide/osqt/virtual"
)

var (
	listenAddr    string
	targetOS      string
	serveCommands = []cli.Command{
		{
			Name:  "run",
			Usage: "Launches a MySQL compatible server with OSQuery tables setup.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "listen-addr",
					Destination: &listenAddr,
					Value:       "127.0.0.1:13306",
					Usage:       "Sets the listening server socket that will accept MySQL connections.",
					EnvVar:      "OSQT_LISTENING_ADDR",
				},
				cli.StringFlag{
					Name:        "schema",
					Destination: &schemaPath,
					Usage:       "Path to a previously exported OSQuery schema JSON or YAML file.",
					EnvVar:      "OSQT_SCHEMA_PATH",
				},
				cli.StringFlag{
					Name:        "specs-dir",
					Destination: &specsDir,
					Usage:       "User defined query to be used in OSQuery (required)",
					EnvVar:      "OSQT_SPECS_DIR",
				},
				cli.StringFlag{
					Name:        "target-os",
					Value:       runtime.GOOS,
					Destination: &targetOS,
					Usage:       "Runtime to target for the OSQuery dynamic configuration (what tables to use).",
					EnvVar:      "OSQT_TARGET_OS",
				},
			},
			Action: runServer,
		},
	}
)

func runServer(c *cli.Context) error {
	if schemaPath == "" && specsDir == "" {
		return xerrors.New("--schema PATH or --specs-dir PATH are required!")
	}

	parser := osqt.NewParser(log.Named("parser"))
	if specsDir != "" {
		err := parser.ParseDirectory(specsDir)
		if err != nil {
			return err
		}
	} else {
		switch filepath.Ext(schemaPath) {
		case ".json":
			err := parser.ParseJSONSchemaFile(schemaPath)
			if err != nil {
				return err
			}
		case ".yaml":
			err := parser.ParseYAMLSchemaFile(schemaPath)
			if err != nil {
				return err
			}
		}
	}

	db, err := virtual.NewDatabase("vosqt", parser, log.Named("db"))
	if err != nil {
		return err
	}

	namespaces, found := osqt.GOOSToApplicableNamespaces[targetOS]
	if !found {
		return xerrors.Errorf("--target-os value provided (%s) was not valid (valid: 'windows', 'linux', 'darwin', 'freebsd').", targetOS)
	}

	for _, nsid := range namespaces {
		ns, valid := parser.Namespaces[nsid]
		if !valid {
			log.Errorf("could not locate %s namespace within the parser", nsid)
			continue
		}

		for tblname, table := range ns.Tables {
			err := db.AddTable(table, []string{targetOS})
			if err != nil {
				log.Errorf("Error encountered adding a table to the database: %v", err)
				continue
			}
			log.Debugf("Added table %s to the database...", tblname)
		}
	}

	err = db.Initialize()
	if err != nil {
		return err
	}

	log.Infof("Starting server listener at: %s", listenAddr)
	err = db.Start("tcp", listenAddr)
	if err != nil {
		return err
	}

	return nil
}

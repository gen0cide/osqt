package osqt

import (
	"fmt"

	past "github.com/go-python/gpython/ast"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

// CanonicalPlatforms lists the possible platform subfolders within a table.
var CanonicalPlatforms = map[string]string{
	"specs":     "All Platforms",
	"darwin":    "Darwin (Apple OS X)",
	"linux":     "Ubuntu, CentOS",
	"freebsd":   "FreeBSD",
	"posix":     "POSIX-compatible Plaforms",
	"windows":   "Microsoft Windows",
	"utility":   "Utility",
	"yara":      "YARA",
	"smart":     "SMART",
	"lldpd":     "LLDPD",
	"sleuthkit": "The Sleuth Kit",
	"macwin":    "MacOS and Windows",
	"linwin":    "Linux and Windows",
}

// GOOSToApplicableNamespaces is a helper to let you lookup OSQuery namespaces relating to a given GOOS runtime.
var GOOSToApplicableNamespaces = map[string][]string{
	"linux": []string{
		"specs",
		"linux",
		"posix",
		"utility",
		"yara",
		"smart",
		"lldpd",
		"sleuthkit",
		"linwin",
	},
	"darwin": []string{
		"specs",
		"posix",
		"utility",
		"darwin",
		"yara",
		"smart",
		"lldpd",
		"sleuthkit",
		"macwin",
	},
	"windows": []string{
		"specs",
		"windows",
		"utility",
		"yara",
		"smart",
		"sleuthkit",
		"macwin",
		"linwin",
	},
	"freebsd": []string{
		"specs",
		"freebsd",
		"posix",
		"utility",
		"yara",
		"smart",
		"lldpd",
	},
}

// TableCategories are used to apply applicable platforms to extended schema definitions.
var TableCategories = map[string][]string{
	"WINDOWS": []string{
		"windows",
		"win32",
		"cygwin",
	},
	"LINUX": []string{
		"linux",
	},
	"POSIX": []string{
		"linux",
		"darwin",
		"freebsd",
	},
	"DARWIN": []string{
		"darwin",
	},
	"FREEBSD": []string{
		"freebsd",
	},
}

// Schema outlines the structure of the columns within an OSQuery table.
type Schema struct {
	logger *zap.SugaredLogger

	Table       *Table                   `json:"-" yaml:"-"`
	Platforms   []string                 `json:"platforms,omitempty" yaml:"platforms,omitempty"`
	Extended    bool                     `json:"extended,omitempty" yaml:"extended,omitempty"`
	Columns     []*Column                `json:"columns,omitempty" yaml:"columns,omitempty"`
	ForeignKeys []map[string]interface{} `json:"foreign_keys,omitempty" yaml:"foreign_keys,omitempty"`
}

// NewEmptySchema returns an initialized, but empty Schema object.
func NewEmptySchema(t *Table) *Schema {
	return &Schema{
		Table:       t,
		Platforms:   []string{},
		Columns:     []*Column{},
		ForeignKeys: []map[string]interface{}{},
	}
}

// TableName returns the name of the parent table, while accounting for nil pointers.
func (s *Schema) TableName() string {
	if s.Table == nil {
		return "UNKNOWN (nil table)"
	}
	return s.Table.Name
}

// Logger returns a logger for a given schema and tries to base it off it's parent table's logger if possible.
func (s *Schema) Logger() *zap.SugaredLogger {
	if s.logger == nil {
		if s.Table == nil {
			s.logger = zap.L().Sugar().Named("undefined_schema")
		} else {
			s.logger = s.Table.Logger().Named("schema")
		}
	}

	return s.logger
}

// ParseLambda attempts to extract the logical OR values out of the custom expression to identify applicable platforms.
func (s *Schema) ParseLambda(lambda *past.Lambda) error {
	bodyOp, ok := lambda.Body.(*past.BoolOp)
	if !ok {
		err := xerrors.Errorf("lambda body type mismatch: expected *ast.BoolOp, got %T", lambda.Body)
		s.Logger().Errorw("Schema parsing error", "error", err)
		return err
	}
	if bodyOp.Op != past.Or {
		err := xerrors.Errorf("lambda body operation mismatch: expected OR (2), got %v", bodyOp.Op.String())
		s.Logger().Errorw("Schema parsing error", "error", err)
		return err
	}
	for _, valast := range bodyOp.Values {
		bodycall, ok := valast.(*past.Call)
		if !ok {
			err := xerrors.Errorf("lambda OR value mismatch: expected *ast.Call, got %T", valast)
			s.Logger().Errorw("Schema parsing error", "error", err)
			return err
		}
		funcident, ok := bodycall.Func.(*past.Name)
		if !ok {
			err := xerrors.Errorf("lambda OR value function mismatch: expected *ast.Name, got %T", bodycall.Func)
			s.Logger().Errorw("Schema parsing error", "error", err)
			return err
		}
		platformList, ok := TableCategories[string(funcident.Id)]
		if !ok {
			err := xerrors.Errorf("No table category for provided function identifier: %s", string(funcident.Id))
			s.Logger().Errorw("Schema parsing error", "error", err)
			return err
		}
		tmp := map[string]bool{}
		for _, elm := range s.Platforms {
			tmp[elm] = true
		}
		for _, elm := range platformList {
			tmp[elm] = true
		}
		res := make([]string, len(tmp))
		idx := 0
		for key := range tmp {
			res[idx] = key
			idx++
		}
		s.Platforms = res
	}

	return nil
}

// ExtractSchema attempts to extract the schema([]) declaraction.
func (s *Schema) ExtractSchema(node *past.Call) error {
	argsIndex := 0
	callerFuncName, ok := node.Func.(*past.Name)
	if !ok {
		err := xerrors.Errorf("expected caller function name to be of type (*past.Name), was %T", node.Func)
		s.Logger().Errorw("Schema parsing error", "error", err)
		return err
	}

	if string(callerFuncName.Id) == "extended_schema" {
		s.Extended = true
		argsIndex = 1
		switch platformArg := node.Args[0].(type) {
		case *past.NameConstant:
			platformList, ok := TableCategories[fmt.Sprintf("%v", platformArg.Value)]
			if !ok {
				err := xerrors.Errorf("No table category for provided function identifier: %s", fmt.Sprintf("%v", platformArg.Value))
				s.Logger().Errorw("Schema parsing error", "error", err)
				return err
			}
			tmp := map[string]bool{}
			for _, elm := range s.Platforms {
				tmp[elm] = true
			}
			for _, elm := range platformList {
				tmp[elm] = true
			}
			res := make([]string, len(tmp))
			idx := 0
			for key := range tmp {
				res[idx] = key
				idx++
			}
			s.Platforms = res
		case *past.Str:
			platformList, ok := TableCategories[string(platformArg.S)]
			if !ok {
				err := xerrors.Errorf("No table category for provided function identifier: %s", string(platformArg.S))
				s.Logger().Errorw("Schema parsing error", "error", err)
				return err
			}
			tmp := map[string]bool{}
			for _, elm := range s.Platforms {
				tmp[elm] = true
			}
			for _, elm := range platformList {
				tmp[elm] = true
			}
			res := make([]string, len(tmp))
			idx := 0
			for key := range tmp {
				res[idx] = key
				idx++
			}
			s.Platforms = res
		case *past.Name:
			platformList, ok := TableCategories[string(platformArg.Id)]
			if !ok {
				err := xerrors.Errorf("No table category for provided function identifier: %s", string(platformArg.Id))
				s.Logger().Errorw("Schema parsing error", "error", err)
				return err
			}
			tmp := map[string]bool{}
			for _, elm := range s.Platforms {
				tmp[elm] = true
			}
			for _, elm := range platformList {
				tmp[elm] = true
			}
			res := make([]string, len(tmp))
			idx := 0
			for key := range tmp {
				res[idx] = key
				idx++
			}
			s.Platforms = res
		case *past.Lambda:
			err := s.ParseLambda(platformArg)
			if err != nil {
				return err
			}
		default:
			err := xerrors.Errorf("could not determine type for extended_schema platform argument: %v (%T)", platformArg, platformArg)
			s.Logger().Errorw("Schema parsing error", "error", err)
			return err
		}
	}

	arglist, ok := node.Args[argsIndex].(*past.List)
	if !ok {
		err := xerrors.Errorf("argument %d was not of type *arg.List (extended=%v)", argsIndex, s.Extended)
		s.Logger().Errorw("Schema parsing error", "error", err)
		return err
	}
	for colidx, coldef := range arglist.Elts {
		coldefcaller, ok := coldef.(*past.Call)
		if !ok {
			err := xerrors.Errorf("argument %d was not of type *ast.Call", colidx)
			s.Logger().Errorw("Schema parsing error", "error", err)
			return err
		}

		funcName, ok := coldefcaller.Func.(*past.Name)
		if !ok {
			err := xerrors.Errorf("Column definition caller function at index %d was not a *past.Name", colidx)
			s.Logger().Errorw("Schema parsing error", "error", err)
			return err
		}

		if string(funcName.Id) == "ForeignKey" {
			fkey := map[string]interface{}{}
			for _, kw := range coldefcaller.Keywords {
				optkey := string(kw.Arg)
				switch v := kw.Value.(type) {
				case *past.NameConstant:
					fkey[optkey] = v.Value
				case *past.Str:
					fkey[optkey] = string(v.S)
				case *past.Name:
					fkey[optkey] = string(v.Id)
				}
			}
			s.ForeignKeys = append(s.ForeignKeys, fkey)
			continue
		}

		col := NewEmptyColumn()
		col.Index = colidx

		if len(coldefcaller.Args) < 1 {
			s.Logger().Warnf("Non Column() definition detected! (function=%s) Skipping...", string(funcName.Id))
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

		s.Columns = append(s.Columns, col)
	}
	return nil
}

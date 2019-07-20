package osqt

import "go.uber.org/zap"

// Namespace is a container to hold compatibility information about an OSQuery table set.
type Namespace struct {
	logger *zap.SugaredLogger
	parser *Parser

	Key    string            `json:"key,omitempty" yaml:"key,omitempty"`
	Name   string            `json:"name,omitempty" yaml:"name,omitempty"`
	Tables map[string]*Table `json:"tables,omitempty" yaml:"tables,omitempty"`
}

// Logger will return a zap Logger, creating one if the Namespace has no previous Logger defined.
func (n *Namespace) Logger() *zap.SugaredLogger {
	if n.logger == nil {
		if n.parser == nil || n.parser.Logger == nil {
			n.logger = zap.L().Sugar().Named("parser").Named(n.Key)
		} else {
			n.logger = n.parser.Logger.Named(n.Key)
		}
	}

	return n.logger
}

// NewNamespace is used to create a new namespace container to hold OSQuery tables.
func NewNamespace(key, name string, parser *Parser, logger *zap.SugaredLogger) *Namespace {
	if logger == nil && parser.Logger != nil {
		logger = parser.Logger.Named(key)
	} else if logger == nil && parser.Logger == nil {
		logger = zap.L().Sugar().Named("parser").Named(key)
	}
	return &Namespace{
		logger: logger,
		parser: parser,
		Key:    key,
		Name:   name,
		Tables: map[string]*Table{},
	}
}

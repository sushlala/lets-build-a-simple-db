package statement

import (
	"errors"
	"github.com/sussadag/lets-build-a-simple-db/table"
	"strings"
)

// Error Codes
var (
	ErrUnrecognizedStatement = errors.New("statement not recognized")
	ErrSyntaxError           = errors.New("syntax error. Could not parse statement")
)

const (
	SELECT = iota
	INSERT
)

type statement interface {
	Execute(*table.Table) error
}

// Prepare parses the sql cmd query into
// a statement which it returns
func Prepare(cmd string, t *table.Table) (s statement, err error) {
	if strings.HasPrefix(cmd, "select") {
		return prepareSelect(cmd)
	} else if strings.HasPrefix(cmd, "insert") {
		return prepareInsert(cmd)
	}
	return nil, ErrUnrecognizedStatement
}

// Execute the returned statement s
func Execute(s statement, t *table.Table) error {
	return s.Execute(t)
}

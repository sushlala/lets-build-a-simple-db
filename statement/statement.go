package statement

import (
	"errors"
	"fmt"
	"strings"
)

// Error Codes
var (
	ErrUnrecognizedStatement = errors.New("statement not recognized")
)

const (
	SELECT = iota
	INSERT
)


type statement struct {
	statementType uint
}

// Prepare parses the sql cmd query into
// a statement which it returns
func Prepare(cmd string) (*statement, error){
	if strings.HasPrefix(cmd, "select"){
		return &statement{SELECT}, nil
	}
	if strings.HasPrefix(cmd, "insert"){
		return &statement{INSERT}, nil
	}
	return nil, ErrUnrecognizedStatement
}

// Execute the returned statement s
func Execute(s *statement) error {
	switch s.statementType {
	case SELECT:
		fmt.Print("This is where the select statement would be\n")
	case INSERT:
		fmt.Print("This is where the insert statement would be\n")
	default:
		return ErrUnrecognizedStatement
	}
	return nil
}
package statement

import (
	"errors"
	"fmt"
	"github.com/sussadag/lets-build-a-simple-db/table"
	"strings"
)

// insert specific errors
var (
	ErrStringTooLong = errors.New("string too long")
	ErrNegativeId    = errors.New("negative id")
)

type insertStatement struct {
	r table.Row
}

func prepareInsert(cmd string) (*insertStatement, error) {
	s := insertStatement{}
	var tmp, user, email string
	n, err := fmt.Fscanln(
		strings.NewReader(cmd),
		&tmp,
		&s.r.Id,
		&user,
		&email,
	)
	if err != nil {
		return nil, ErrSyntaxError
	}
	if n != 4 {
		return nil, ErrSyntaxError
	}
	if len(user) > len(s.r.Username) || len(email) > len(s.r.Email) {
		return nil, ErrStringTooLong
	}
	if s.r.Id < 0 {
		return nil, ErrNegativeId
	}

	copy(s.r.Username[:], []byte(user))
	copy(s.r.Email[:], []byte(email))
	return &s, nil
}

func (s *insertStatement) Execute(t *table.Table) error {
	err := t.Insert(s.r)
	if err == table.ErrTableFull {
		return ErrTableFull
	}
	return err
}

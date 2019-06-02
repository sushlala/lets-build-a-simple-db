package statement

import (
	"fmt"
	"github.com/sussadag/lets-build-a-simple-db/table"
	"strings"
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
	copy(s.r.Username[:], []byte(user))
	copy(s.r.Email[:], []byte(email))
	return &s, nil
}

func (s *insertStatement) Execute(t *table.Table) error {
	return t.Insert(s.r)
}

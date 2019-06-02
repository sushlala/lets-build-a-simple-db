package statement

import (
	"bytes"
	"fmt"
	"github.com/sussadag/lets-build-a-simple-db/table"
)

type selectStatement struct {
}

func prepareSelect(cmd string) (*selectStatement, error) {
	return &selectStatement{}, nil
}

func (s *selectStatement) Execute(t *table.Table) error {
	for r := range t.GetRows() {
		fmt.Printf(
			"(%d, %s, %s)\n",
			r.Id,
			// remove extra null characters in returned
			// byte arrays
			bytes.TrimRight(r.Username[:], "\x00"),
			bytes.TrimRight(r.Email[:], "\x00"),
		)
	}
	return nil
}

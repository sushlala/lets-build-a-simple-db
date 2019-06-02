package statement

import (
	"fmt"
	"github.com/sussadag/lets-build-a-simple-db/table"
)

type selectStatement struct {
}

func prepareSelect(cmd string) (*selectStatement, error) {
	return &selectStatement{}, nil
}

func convertToPrintableRow(r table.Row) []string {
	out := make([]string, 3)
	out = append(out, string(r.Id))
	out = append(out, string(r.Username[:32]))
	out = append(out, string(r.Email[:256]))
	return out
}
func (s *selectStatement) Execute(t *table.Table) error {
	for r := range t.GetRows() {
		fmt.Printf("(%d, %s, %s)\n", r.Id, r.Username, r.Email)
	}
	return nil
}

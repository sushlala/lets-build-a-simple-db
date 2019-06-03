package metacmd

import (
	"errors"
	"github.com/sussadag/lets-build-a-simple-db/table"
	"log"
	"os"
)

// Error Codes
var (
	ErrUnrecognizedCmd = errors.New("meta command not recognized")
)

// Execute performs the meta command in cmd
func Execute(cmd string, t *table.Table) error {
	switch cmd {
	case ".exit":
		if err := t.CloseDb(); err != nil {
			log.Fatalf("Failed to close the database: '%s'", err)
		}
		os.Exit(0)
	default:
		return ErrUnrecognizedCmd
	}
	return nil
}

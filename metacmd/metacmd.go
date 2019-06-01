package metacmd

import (
	"errors"
	"os"
)

// Error Codes
var (
	ErrUnrecognizedCmd = errors.New("meta command not recognized")
)

// Execute performs the meta command in cmd
func Execute(cmd string) error {
	switch cmd {
	case ".exit":
		os.Exit(0)
	default:
		return ErrUnrecognizedCmd
	}
	return nil
}

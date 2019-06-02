package main

import (
	"bufio"
	"fmt"
	"github.com/sussadag/lets-build-a-simple-db/metacmd"
	"github.com/sussadag/lets-build-a-simple-db/statement"
	"github.com/sussadag/lets-build-a-simple-db/table"
	"io"
	"log"
	"os"
	"strings"
)

func printPrompt() {
	fmt.Printf("db >")
}

func getCommand(input *bufio.Reader) (text string) {
	text, err := input.ReadString('\n')
	if err == io.EOF {
		os.Exit(0)
	}
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(text, "\n", "", -1)
}

func main() {
	input := bufio.NewReader(os.Stdin)
	t := table.NewTable()
	for {
		printPrompt()
		text := getCommand(input)
		if strings.HasPrefix(text, ".") {
			// handle meta command
			if err := metacmd.Execute(text); err != nil {
				switch err {
				case metacmd.ErrUnrecognizedCmd:
					fmt.Printf("Unrecognized command '%s'\n", text)
				default:
					log.Fatalf("Failed to execute command '%s'", err)
				}
			}
			continue
		}
		// handle sql statement
		s, err := statement.Prepare(text, t)
		switch err {
		case statement.ErrUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", text)
			continue
		case statement.ErrSyntaxError:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case statement.ErrStringTooLong:
			fmt.Println("String is too long.")
			continue
		case statement.ErrNegativeId:
			fmt.Println("ID must be positive.")
			continue
		}
		if err != nil {
			fmt.Printf("Unexpected error '%s", err)
			continue
		}

		// Execute prepared statement
		err = statement.Execute(s, t)
		switch err {
		case statement.ErrTableFull:
			fmt.Println("Error: Table full.")
			continue
		}
		if err != nil {
			log.Fatalf("Fatal error while executing '%s', error '%s'", text, err)
		}
		fmt.Printf("Executed.\n")
	}

}

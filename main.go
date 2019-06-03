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

func getDbFileName() string {
	if len(os.Args) < 2 {
		log.Fatalf("Must supply a database filename")
	}
	return os.Args[1]
}

func main() {
	dbFileName := getDbFileName()
	input := bufio.NewReader(os.Stdin)
	t, err := table.OpenDb(dbFileName)
	if err != nil {
		log.Fatalf("Failed to open the db: '%s'", err)
	}
	for {
		printPrompt()
		line := getCommand(input)
		if strings.HasPrefix(line, ".") {
			// handle meta command
			if err := metacmd.Execute(line, t); err != nil {
				switch err {
				case metacmd.ErrUnrecognizedCmd:
					fmt.Printf("Unrecognized command '%s'\n", line)
				default:
					log.Fatalf("Failed to execute command '%s'", err)
				}
			}
			continue
		}
		// handle sql statement
		s, err := statement.Prepare(line, t)
		switch err {
		case statement.ErrUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'\n", line)
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
			log.Fatalf("Error while executing statement: '%s'", err)
		}
		fmt.Printf("Executed.\n")
	}

}

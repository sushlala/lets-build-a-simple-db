package main

import (
	"bufio"
	"fmt"
	"github.com/sussadag/lets-build-a-simple-db/metacmd"
	"github.com/sussadag/lets-build-a-simple-db/statement"
	"log"
	"os"
	"strings"
)

func printPrompt() {
	fmt.Printf("db >")
}

func getCommand(input *bufio.Reader) (text string) {
	text, err := input.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(text, "\n", "", -1)
}

func main() {
	input := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		text := getCommand(input)
		if strings.HasPrefix(text, ".") {
			// handle meta command
			if err := metacmd.Execute(text); err != nil {
				switch err {
				case metacmd.ErrUnrecognizedCmd :
						fmt.Printf("Unrecognized command '%s'\n", text)
				default:
					log.Fatalf("Failed to execute command '%s'", err)
				}
			}
			continue
		}
		// handle sql statement
		s, err  := statement.Prepare(text)
		if err == statement.ErrUnrecognizedStatement{
			fmt.Printf("Unrecognized keyword at start of '%s'\n", text)
			continue
		}
		err = statement.Execute(s)
		if err != nil{
			log.Fatalf("Fatal error while executing '%s', error '%s'", text, err)
		}
		fmt.Printf("Executed.\n")
	}

}

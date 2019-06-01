package main

import (
	"bufio"
	"fmt"
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
		switch text {
		case ".exit":
			os.Exit(0)

		default:
			fmt.Printf("Unrecognized command '%s'\n", text)
		}
	}

}

package behavior_test

// Simply run using `go test ./... -v`

import (
	"bytes"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

func runCommands(cmds []string) []string {
	cmd := exec.Command("go", "run", "../main.go")

	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb

	stdin, err := cmd.StdinPipe()
	if err != nil {
		fmt.Println(err) //replace with logger, or anything you want
	}

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	for _, c := range cmds {
		//fmt.Printf("exec command %s", c)
		c += "\n"
		_, err = stdin.Write([]byte(c))
		if err != nil {
			log.Fatal(err)
		}

	}

	err = cmd.Wait()
	if err != nil {
		log.Fatal(err)
	}

	out := outb.String()

	return strings.Split(out, "\n")
}

func TestInsertOneRow(t *testing.T) {
	Convey("database behaves correctly", t, func() {

		Convey("on inserting a row returns it", func() {
			cmds := []string{
				"insert 1 user1 person1@example.com",
				"select",
				".exit"}
			output := runCommands(cmds)
			So(output, ShouldResemble, []string{"db >Executed.",
				"db >(1, user1, person1@example.com)",
				"Executed.",
				"db >"})

		})

		Convey("prints error message when table is full", func() {
			cmds := []string{}
			for i := 1; i <= 1301; i++ {
				cmds = append(
					cmds,
					"insert "+strconv.Itoa(i)+"someuser some@email.com",
				)
			}
			cmds = append(cmds, ".exit")
			output := runCommands(cmds)
			So(output[len(output)-2], ShouldEqual, "db >Error: Table full.")
		})

		Convey("allows inserting strings that are the maximum length", func() {
			longUsername := strings.Repeat("a", 32)
			longEmail := strings.Repeat("a", 256)

			cmds := []string{
				"insert 1 " + longUsername + " " + longEmail,
				"select",
				".exit",
			}
			output := runCommands(cmds)

			So(output[len(output)-3], ShouldEqual, "db >(1, "+longUsername+", "+longEmail+")")
		})

		Convey("prints error message if strings are too long", func() {
			longUsername := strings.Repeat("a", 33)
			longEmail := strings.Repeat("a", 257)

			cmds := []string{
				"insert 1 " + longUsername + " " + longEmail,
				"select",
				".exit",
			}
			output := runCommands(cmds)

			So(output[len(output)-3], ShouldEqual, "db >String is too long.")
		})
		Convey("prints an error message if id is negative", func() {
			cmds := []string{
				"insert -1 user name@domain.com",
				"select",
				".exit",
			}
			output := runCommands(cmds)

			So(output[len(output)-3], ShouldEqual, "db >ID must be positive.")
		})

	})

}

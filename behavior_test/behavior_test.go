package behavior_test

// Simply run using `go test ./... -v`

import (
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

func runCommands(cmds []string, dbfile string) []string {
	cmd := exec.Command("go", "run", "../main.go", dbfile)

	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb

	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
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

func TestSpec(t *testing.T) {
	dbFile := "tmp.db"
	Convey("database behaves correctly", t, func() {

		Convey("on inserting a row returns it", func() {
			cmds := []string{
				"insert 1 user1 person1@example.com",
				"select",
				".exit"}
			output := runCommands(cmds, dbFile)
			defer func() {
				os.Remove(dbFile)
			}()

			So(
				output,
				ShouldResemble,
				[]string{
					"db >Executed.",
					"db >(1, user1, person1@example.com)",
					"Executed.",
					"db >",
				},
			)

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
			dbFile := "tmp.db"
			output := runCommands(cmds, dbFile)
			defer func() {
				os.Remove(dbFile)
			}()
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
			dbFile := "tmp.db"
			output := runCommands(cmds, dbFile)
			defer func() {
				os.Remove(dbFile)
			}()

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
			dbFile := "tmp.db"
			output := runCommands(cmds, dbFile)
			defer func() {
				os.Remove(dbFile)
			}()

			So(output[len(output)-3], ShouldEqual, "db >String is too long.")
		})
		Convey("prints an error message if id is negative", func() {
			cmds := []string{
				"insert -1 user name@domain.com",
				"select",
				".exit",
			}
			output := runCommands(cmds, dbFile)
			defer func() {
				os.Remove(dbFile)
			}()

			So(output[len(output)-3], ShouldEqual, "db >ID must be positive.")
		})

		Convey("keeps data after closing connection", func() {

			Convey("insert one item and close connection", func() {
				cmds := []string{
					"insert 1 user1 person1@example.com",
					".exit",
				}
				output := runCommands(cmds, dbFile)
				So(
					output,
					ShouldResemble,
					[]string{
						"db >Executed.",
						"db >",
					},
				)
			})
			Convey("the item exists in a new connection", func() {
				cmds := []string{
					"select",
					".exit",
				}
				output := runCommands(cmds, dbFile)
				defer func() {
					os.Remove(dbFile)
				}()
				So(
					output[0],
					ShouldEqual,
					"db >(1, user1, person1@example.com)",
				)
			})
			Convey("insert 20 items and close connection", func() {
				cmds := []string{}
				for i := 1; i <= 20; i++ {
					cmds = append(cmds, "insert "+strconv.Itoa(i)+" user1 person1@example.com")
				}
				cmds = append(cmds, ".exit")
				output := runCommands(cmds, dbFile)
				So(
					output[len(output)-2],
					ShouldEqual,
					"db >Executed.",
				)
			})
			Convey("20th item exists in a new connection", func() {
				cmds := []string{
					"select",
					".exit",
				}
				output := runCommands(cmds, dbFile)
				defer func() {
					os.Remove(dbFile)
				}()
				So(
					output[len(output)-3],
					ShouldEqual,
					"(20, user1, person1@example.com)",
				)
			})

		})

	})

}

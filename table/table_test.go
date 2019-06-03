package table_test

import (
	"github.com/sussadag/lets-build-a-simple-db/table"
	"log"
	"os"
	"testing"
)

func TestInsertOneRow(t *testing.T) {

	var nameArr [32]byte
	{
		a := "sush"
		copy(nameArr[:], []byte(a)[:])
	}
	var emailArr [256]byte
	{
		a := "sush@lala.com"
		copy(emailArr[:], []byte(a)[:])
	}
	r := table.Row{
		Id:       1,
		Username: nameArr,
		Email:    emailArr,
	}

	tab, err := table.OpenDb("temp.db")
	defer func() {
		os.Remove("temp.db")
	}()

	if err != nil {
		t.Fatal(err)
	}
	err = tab.Insert(r)
	if err != nil {
		t.Fatal(err)
	}

	for i := range tab.GetRows() {
		if i.Err != nil {
			t.Fatal(i.Err)
		}
		if r != i.Row {
			t.Logf("Got '% x', sent '% x'", i.Row, r)
			t.Fatalf("Did not get what we put in")
		}
	}
}

func TestInsertIntoTwoPages(t *testing.T) {

	var nameArr [32]byte
	{
		a := "sush"
		copy(nameArr[:], []byte(a)[:])
	}
	var emailArr [256]byte
	{
		a := "sush@lala.com"
		copy(emailArr[:], []byte(a)[:])
	}

	tab, err := table.OpenDb("temp.db")
	defer func() {
		os.Remove("temp.db")
	}()
	if err != nil {
		t.Fatal(err)
	}
	numRows := 20
	for i := 1; i <= numRows; i++ {
		err := tab.Insert(
			table.Row{
				Id:       int64(i),
				Username: nameArr,
				Email:    emailArr,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	out := int64(1)
	for i := range tab.GetRows() {
		if i.Err != nil {
			log.Fatal(i.Err)
		}
		r2 := i.Row
		if r2.Id != out {
			t.Fatalf(
				"Unexpected value. "+
					"Expected row with id %d, got row '%+v'",
				out, r2)

		}
		out += 1
	}
	if int(out-1) != numRows {
		t.Fatalf(
			"Failed to get back %d rows, only got %d",
			numRows, out)
	}
}

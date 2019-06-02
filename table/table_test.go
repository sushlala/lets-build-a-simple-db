package table_test

import (
	"github.com/sussadag/lets-build-a-simple-db/table"
	"log"
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

	tab := table.NewTable()
	err := tab.Insert(r)
	if err != nil {
		t.Fatal(err)
	}

	for r2 := range tab.GetRows() {
		if r != r2 {
			t.Logf("Got '% x', sent '% x'", r2, r)
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

	tab := table.NewTable()
	numRows := 20
	for i := 1; i <= numRows; i++ {
		log.Printf("inserting row with id %d", i)
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
	for r2 := range tab.GetRows() {
		if r2.Id != out {
			t.Fatalf(
				"Unexpected value. "+
					"Expected row with id %d, got row '%+v'",
				out, r2)

		}
		log.Printf("Got row with id %d", out)
		out += 1
	}
	if int(out-1) != numRows {
		t.Fatalf(
			"Failed to get back %d rows, only got %d",
			numRows, out)
	}
}

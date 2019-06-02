package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unsafe"
)

// Table is not thread safe!

// Table implements the append-only single in-memory Table that
// consists of rows of entries:
//		column			type
// 		========== 		==============
// 		id				integer
//		username		varchar(32)
//		email			varchar(255)

// Row in our append-only Table
type Row struct {
	Id       int64
	Username [32]byte
	// Changed this from 255 (value in the original write-up)
	// to 256 to ensure that the size of struct and the binary
	// representation are the same. In other words, there is no
	// padding inserted in this struct by the compiler
	Email [256]byte
}

const (
	pageSize = 4096
	// maxNumPages is an arbitrary limit for now
	// eventually we'll only be bound by the size of
	// backing physical storage
	maxNumPages = 100
	rowSize     = uint(unsafe.Sizeof(Row{}))
	rowsPerPage = pageSize / rowSize
)

type page [pageSize]byte

// Table is an instance of our in-memory append-only
// table with just one schema
type Table struct {
	// current number of rows in Table
	nextFreeRow uint
	// pointer to pages that contain
	// rows of data
	pages [maxNumPages]*page
}

// NewTable returns the single
// Table that we maintain in-memory
func NewTable() *Table {
	return &Table{}
}

var (
	ErrTableFull = errors.New("table full")
)

// insertIntoPage marshals r into p as the indexInPage element
func insertIntoPage(p *page, r Row, indexInPage uint) {
	// marshal r in to binary form
	binaryR := new(bytes.Buffer)
	err := binary.Write(binaryR, binary.LittleEndian, r)
	if err != nil {
		panic("failed to marshal row")
	}
	if uint(len(binaryR.Bytes())) != rowSize {
		// struct has some extra padding!
		panic("size of binary representation different from struct size")
	}
	copy(
		p[indexInPage*rowSize:],
		binaryR.Bytes(),
	)
}

// getRowLocation gets the location of where to store the new
// page into the Table
func getRowLocation(rowNum uint) (pageNum, indexInPage uint) {
	pageNum = rowNum / rowsPerPage
	indexInPage = rowNum % rowsPerPage
	return
}

// Insert tries to insert into the Table
func (t *Table) Insert(r Row) error {
	pageNum, indexInPage := getRowLocation(t.nextFreeRow)
	if pageNum >= maxNumPages {
		return ErrTableFull
	}
	p := t.pages[pageNum]
	if p == nil {
		// only allocate a new page when needed

		p = &page{}
		t.pages[pageNum] = p
	}
	insertIntoPage(p, r, indexInPage)
	t.nextFreeRow += 1
	return nil
}

// readPage reads Rows off p and emits them to c
func readPage(p *page, c chan<- Row) {

	bArray := [pageSize]byte(*p)
	bSlice := bArray[:]
	bReader := bytes.NewReader(bSlice)
	r := &Row{}
	emptyRow := Row{}
	var numRead uint = 0
	for {
		if numRead > rowsPerPage {
			break
		}
		err := binary.Read(bReader, binary.LittleEndian, r)
		if err == io.ErrUnexpectedEOF ||
			// empty row means we hit end of contents on this page
			*r == emptyRow {
			break
		}
		if err != nil {
			panic(err)
		}

		c <- *r
		numRead += 1
	}
	return
}
func (t *Table) GetRows() <-chan Row {
	c := make(chan Row)
	go func() {
		for _, p := range t.pages {
			if p == nil {
				// no more pages in our append-only Table
				close(c)
				return
			}
			readPage(p, c)
		}
		// every page had contents
		close(c)
	}()
	return c
}

package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"unsafe"
)

// Table is not thread safe!

// Table implements the append-only single in-memory Table that
// consists of rows of entries:
//		column			type
// 		========== 		==============
// 		id				integer
//		username		varchar(32)
//		email			varchar(256)

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
	rowSize     = uint(unsafe.Sizeof(Row{})) // is 296
	rowsPerPage = pageSize / rowSize         // is 13
)

type page [pageSize]byte

// Table is an instance of our in-memory append-only
// table with just one schema
type Table struct {
	// current number of rows in Table
	nextFreeRow uint
	p           *pager
}

// OpenDb opens a connection to the database
// in the form of the only table we support
func OpenDb(filename string) (*Table, error) {
	p, err := newPager(filename)
	if err != nil {
		return nil, err
	}
	t := &Table{
		p:           p,
		nextFreeRow: uint(p.fileSize) / rowSize,
	}
	return t, nil
}

// CloseDb flushes the database to disk
func (t *Table) CloseDb() error {
	return t.p.flushToDisk(t.nextFreeRow)
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
	p, err := t.p.getPage(pageNum)
	log.Printf("got page to insert into at address %p", p)
	if err != nil {
		return err
	}
	insertIntoPage(p, r, indexInPage)
	t.nextFreeRow += 1
	return nil
}

// readPage reads Rows off p and emits them to c
func readPage(p *page, c chan<- GetRowsResult) {

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
		c <- GetRowsResult{
			nil,
			*r,
		}
		numRead += 1
	}
	return
}

type GetRowsResult struct {
	Err error
	Row Row
}

// GetRows provides a handle that emits all rows present
// in insertion order
func (t *Table) GetRows() <-chan GetRowsResult {
	c := make(chan GetRowsResult)
	go func() {
		numPagesInTable := t.nextFreeRow / rowsPerPage
		log.Printf("numPagesInTable=%d", numPagesInTable)
		if t.nextFreeRow%rowsPerPage != 0 {
			// there is a partial page
			log.Printf("partial page! increased number numPagesInTable=%d", numPagesInTable)
			numPagesInTable += 1
		}

		for i := uint(0); i < numPagesInTable; i += 1 {
			p, err := t.p.getPage(i)
			if err != nil {
				c <- GetRowsResult{
					err,
					Row{},
				}
				close(c)
				return
			}
			// readPage knows to stop reading a partial page
			log.Printf("asking to read pageNum %d", i)
			readPage(p, c)
		}
		close(c)
	}()
	return c
}

package table

import (
	"fmt"
	"io"
	"os"
)

// pager stores pages of our table on to disk. It simply
// dumps all the pages into a single database file
//
// It returns the contents of a page when asked for:
//  maintains an internal cache of pages,
//  if it can not find a requested page in the cache
//  it fetches it from disk
type pager struct {
	f        *os.File
	fileSize int64

	// pointer to pages that contain
	// rows of data
	pages [maxNumPages]*page
}

// newPager opens a file on disk that
// stores the data
func newPager(filename string) (*pager, error) {
	f, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_RDWR,
		// http://www.filepermissions.com/file-permission/600
		0600,
	)
	if err != nil {
		return nil, err
	}
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	p := &pager{
		f:        f,
		fileSize: fileInfo.Size(),
	}
	return p, nil
}

// wasPageOnDisk returns true if the
// database file on disk contains the
// page pageNum
func (pag *pager) isPageOnDisk(pageNum uint) bool {
	numRowsOnDisk := uint(pag.fileSize) / rowSize
	numPagesOnDisk := numRowsOnDisk / rowsPerPage

	if numRowsOnDisk%rowsPerPage != 0 {
		numPagesOnDisk += 1
	}
	return pageNum < numPagesOnDisk
}

// copyPageFromDisk copies page at num
// index into p
func (pag *pager) copyPageFromDisk(p *page, num uint) error {
	actualBytesOnDiskPerPage := rowsPerPage * rowSize
	n, err := pag.f.ReadAt(
		[]byte(p[:actualBytesOnDiskPerPage]),
		int64(num*actualBytesOnDiskPerPage),
	)
	if err == io.EOF {
		// we might have read a partial page off disk
		if uint(n)%rowSize != 0 {
			// sanity check: did we read a legal number of bytes?
			return fmt.Errorf("read only %d bytes which is %f rows",
				n, float64(n)/float64(rowsPerPage))
		}
		return nil
	}
	return err
}

// getPage returns the contents of the page at num
//
// If that page was on disk, retrieves it. Else
// returns an empty page
func (pag *pager) getPage(num uint) (*page, error) {
	if num > maxNumPages {
		panic("num > maxNumPages")
	}

	p := pag.pages[num]
	if p == nil {
		// Cache miss. Allocate memory and load from file.
		p = new(page)
		pag.pages[num] = p

		// was this page on disk?
		if pag.isPageOnDisk(num) {
			if err := pag.copyPageFromDisk(p, num); err != nil {
				return nil, err
			}
		} else {
		}
	}
	return p, nil
}

func (pag *pager) flushPartialPage(pageNum, numRows uint) error {
	p := pag.pages[pageNum]
	if p == nil {
		panic("asked to flush nil page to disk")
	}
	_, err := pag.f.WriteAt(
		p[:numRows*rowSize],
		int64(pageNum*rowsPerPage*rowSize),
	)
	return err
}

// flushPageToDisk flushes page at pageNum to disk
func (pag *pager) flushPage(pageNum uint) error {
	return pag.flushPartialPage(pageNum, rowsPerPage)
}

// flushToDisk walks the cache of pages we
// have and flushes them to disk
func (pag *pager) flushToDisk(numRowsInTable uint) error {
	numFullPages := numRowsInTable / rowsPerPage
	for i := uint(0); i < numFullPages; i++ {
		p := pag.pages[i]
		if p == nil {
			continue
		}
		if err := pag.flushPage(i); err != nil {
			return err
		}
	}
	if numRowsInTable%rowsPerPage != 0 {
		// There is a partial page to write to
		// the end of the file
		// For some reason we wont need to do this
		// after we switch to using b-trees
		rowsInLastPage := numRowsInTable % rowsPerPage
		err := pag.flushPartialPage(numFullPages, rowsInLastPage)
		if err != nil {
			return err
		}
	}

	if err := pag.f.Close(); err != nil {
		return err
	}
	return nil
}

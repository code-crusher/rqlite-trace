package store

import (
	"io"
)

// closingReader wraps an io.ReadCloser so that any Read blocked on the
// underlying reader is aborted with ErrClosing as soon as the Store begins
// shutting down.
//
// Reads that feed a snapshot restore can block for a long time -- while
// receiving a snapshot over the network from a Leader, or while streaming a
// large database off local disk. Raft's shutdown does not complete until the
// FSM goroutine returns from an in-flight restore, so without this abort a
// Ctrl+C during a snapshot transfer or node restore is not acted upon until
// the restore finishes. See issue #1695.
type closingReader struct {
	rc        io.ReadCloser
	closingCh <-chan struct{}
}

// readResult carries the result of a Read on the underlying reader. The
// channel carrying these is created per-Read (and buffered), so the goroutine
// performing the underlying Read can always deliver its result and exit.
type readResult struct {
	n   int
	err error
}

// newClosingReader returns a reader that reads from rc, aborting with
// ErrClosing once closingCh is closed.
func newClosingReader(rc io.ReadCloser, closingCh <-chan struct{}) *closingReader {
	return &closingReader{
		rc:        rc,
		closingCh: closingCh,
	}
}

// Read implements io.Reader. If the Store is closing before the read
// completes, ErrClosing is returned immediately.
func (c *closingReader) Read(p []byte) (n int, err error) {
	// Fast path: already closing, don't touch the underlying reader.
	select {
	case <-c.closingCh:
		return 0, ErrClosing
	default:
	}

	readCh := make(chan readResult, 1)
	go func() {
		// The Read on the underlying reader may block for a long time,
		// but once it eventually returns -- the connection is force
		// closed during shutdown -- the result is sent on this
		// buffered channel, so this goroutine never leaks.
		n, err := c.rc.Read(p)
		readCh <- readResult{n, err}
	}()

	select {
	case res := <-readCh:
		return res.n, res.err
	case <-c.closingCh:
		// The pending Read result is buffered, so the goroutine above
		// can deliver it and exit whether or not anyone receives it.
		return 0, ErrClosing
	}
}

// Close implements io.Closer, and closes the underlying ReadCloser.
func (c *closingReader) Close() error {
	return c.rc.Close()
}

// WrappedReadCloser gives access to the underlying ReadCloser, so that
// snapshot.StreamerIndexTerm can still reach the LockingStreamer beneath.
func (c *closingReader) WrappedReadCloser() io.ReadCloser {
	return c.rc
}

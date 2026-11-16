package store

import (
	"errors"
	"io"
	"testing"
	"time"
)

// blockingReader is a reader that never returns data, and never returns
// until it is closed.
type blockingReader struct {
	closed chan struct{}
}

func newBlockingReader() *blockingReader {
	return &blockingReader{closed: make(chan struct{})}
}

func (b *blockingReader) Read(p []byte) (int, error) {
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingReader) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func Test_ClosingReader_NotClosing(t *testing.T) {
	// A closingReader passes reads through while the Store is not closing.
	br := newBlockingReader()
	defer br.Close()

	closingCh := make(chan struct{})
	cr := newClosingReader(br, closingCh)

	go func() {
		time.Sleep(100 * time.Millisecond)
		br.Close()
	}()

	buf := make([]byte, 16)
	done := make(chan error, 1)
	go func() {
		_, err := cr.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("expected io.ErrClosedPipe, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("read did not complete")
	}
}

func Test_ClosingReader_AbortsRead(t *testing.T) {
	// A read blocked on the underlying reader returns ErrClosing as soon
	// as the closing channel is closed.
	br := newBlockingReader()
	defer br.Close()

	closingCh := make(chan struct{})
	cr := newClosingReader(br, closingCh)

	done := make(chan error, 1)
	buf := make([]byte, 16)
	go func() {
		_, err := cr.Read(buf)
		done <- err
	}()

	// Give the blocked read time to be established, then close.
	time.Sleep(100 * time.Millisecond)
	close(closingCh)

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosing) {
			t.Fatalf("expected ErrClosing, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("read was not aborted by closing")
	}

	// Subsequent reads also return ErrClosing immediately.
	if _, err := cr.Read(buf); !errors.Is(err, ErrClosing) {
		t.Fatalf("expected ErrClosing on subsequent read, got %v", err)
	}
}

func Test_ClosingReader_WrappedReadCloser(t *testing.T) {
	// The underlying ReadCloser must remain reachable, so that
	// snapshot.StreamerIndexTerm can still find the streamer beneath.
	br := newBlockingReader()
	defer br.Close()

	closingCh := make(chan struct{})
	cr := newClosingReader(br, closingCh)

	if cr.WrappedReadCloser() != br {
		t.Fatalf("WrappedReadCloser did not return the underlying reader")
	}
}

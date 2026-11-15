package notify

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
)

// testServer is a minimal HTTP server which records notification payloads,
// and calls an optional function for each received payload.
type testServer struct {
	*httptest.Server

	URL string
	fn  notifyFunc
}

// notifyFunc is called for each notification received by a testServer.
type notifyFunc func(payload []byte)

// newTestServer creates, starts, and returns a testServer. fn may be nil.
func newTestServer(fn notifyFunc) *testServer {
	srv := &testServer{
		fn: fn,
	}
	srv.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, err := io.ReadAll(r.Body)
		if err != nil || len(b) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if srv.fn != nil {
			srv.fn(b)
		}
		w.WriteHeader(http.StatusOK)
	}))
	srv.Listener = mustLocalListen()
	srv.Server.Start()
	srv.URL = "http://" + srv.Server.Listener.Addr().String()
	return srv
}

// newFailingTestServer creates and starts a testServer which fails all requests.
func newFailingTestServer() *testServer {
	srv := &testServer{}
	srv.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	srv.Listener = mustLocalListen()
	srv.Server.Start()
	srv.URL = "http://" + srv.Server.Listener.Addr().String()
	return srv
}

func mustLocalListen() net.Listener {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		panic(err)
	}
	return ln
}

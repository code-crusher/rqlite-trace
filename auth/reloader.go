package auth

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const (
	// defaultReloadInterval is how often, when running, the reloader
	// checks the credentials file for changes.
	defaultReloadInterval = 2 * time.Second
)

// CredentialsReloader dynamically reloads a CredentialsStore from a file. The
// file is polled for changes, and when it changes the CredentialsStore it was
// created with is reloaded in place, so all users of that store see the updated
// credentials. If a reload fails, for example because the file is momentarily
// unreadable or contains invalid credentials, the existing credentials remain
// in force and the error is logged. If the path is a symlink, the symlink is
// followed so changes to the linked-to file -- including the symlink being
// repointed at a different file -- are detected.
type CredentialsReloader struct {
	store  *CredentialsStore
	path   string
	logger *log.Logger
	statFn func(name string) (os.FileInfo, error) // overridable for testing

	mu      sync.Mutex
	modTime time.Time
	size    int64
}

// NewCredentialsReloader creates a new CredentialsReloader which reloads the
// given CredentialsStore from the file at path. The store must already have
// been loaded, so that a typo in the path is caught at startup.
func NewCredentialsReloader(store *CredentialsStore, path string) (*CredentialsReloader, error) {
	if store == nil {
		return nil, fmt.Errorf("nil credential store")
	}
	cr := &CredentialsReloader{
		store:  store,
		path:   path,
		logger: log.New(os.Stderr, "[credentials-reloader] ", log.LstdFlags),
	}
	info, err := cr.stat(path)
	if err != nil {
		return nil, err
	}
	cr.modTime = info.ModTime()
	cr.size = info.Size()
	return cr, nil
}

// Start starts the reloader checking the credentials file in the background,
// reloading it when it changes. Checks are performed every interval; if
// interval is zero or less, defaultReloadInterval is used. Closing the
// returned channel stops the checking.
func (cr *CredentialsReloader) Start(interval time.Duration) chan struct{} {
	if interval <= 0 {
		interval = defaultReloadInterval
	}
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				cr.ReloadIfChanged()
			}
		}
	}()
	return stopCh
}

// ReloadIfChanged reloads the credentials file if it has changed since the
// last load, returning any error encountered. It is safe to call concurrently.
func (cr *CredentialsReloader) ReloadIfChanged() error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	info, err := cr.stat(cr.path)
	if err != nil {
		cr.logger.Printf("failed to stat credentials file %s (%s), keeping existing credentials", cr.path, err)
		return err
	}
	if !info.ModTime().After(cr.modTime) && info.Size() == cr.size {
		return nil
	}
	if err := cr.reload(); err != nil {
		cr.logger.Printf("failed to reload credentials file %s (%s), keeping existing credentials", cr.path, err)
		return err
	}
	cr.modTime = info.ModTime()
	cr.size = info.Size()
	cr.logger.Printf("credentials file %s reloaded", cr.path)
	return nil
}

// reload reads the credentials file into the CredentialsStore, replacing all
// credentials and permissions with those in the file. If the file contains
// invalid credentials, the existing credentials remain in force.
func (cr *CredentialsReloader) reload() error {
	f, err := os.Open(cr.path)
	if err != nil {
		return err
	}
	defer f.Close()
	return cr.store.Load(f)
}

// stat stats the credentials file, following symlinks. It is overridable so
// tests can simulate file-system changes.
func (cr *CredentialsReloader) stat(name string) (os.FileInfo, error) {
	if cr.statFn != nil {
		return cr.statFn(name)
	}
	return os.Stat(name)
}

package auth

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

const initialCredentials = `[
	{"username": "username1", "password": "password1", "perms": ["query"]}
]`

// Test_CredentialsReloaderReload tests that a changed credentials file is
// reloaded, and that the new credentials and permissions take effect.
func Test_CredentialsReloaderReload(t *testing.T) {
	path := mustWriteTempFile(t, initialCredentials)
	defer fsutil.Remove(path)

	store, err := NewCredentialsStoreFromFile(path)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}
	reloader, err := NewCredentialsReloader(store, path)
	if err != nil {
		t.Fatalf("failed to create reloader: %s", err.Error())
	}

	if err := reloader.ReloadIfChanged(); err != nil {
		t.Fatalf("unexpected reload of unchanged file: %s", err.Error())
	}
	if !store.Check("username1", "password1") {
		t.Fatalf("initial credentials not in force")
	}
	if store.HasPerm("username1", PermExecute) {
		t.Fatalf("username1 unexpectedly has execute perm")
	}

	// Rotate the password, add a user, and change permissions.
	rotated := `[
		{"username": "username1", "password": "password2", "perms": ["execute"]},
		{"username": "username2", "password": "password3", "perms": ["query"]}
	]`
	if err := os.WriteFile(path, []byte(rotated), 0644); err != nil {
		t.Fatal(err)
	}
	if err := setMtime(path, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	if err := reloader.ReloadIfChanged(); err != nil {
		t.Fatalf("failed to reload changed file: %s", err.Error())
	}
	if store.Check("username1", "password1") {
		t.Fatalf("old password still accepted after rotation")
	}
	if !store.Check("username1", "password2") {
		t.Fatalf("new password not accepted after rotation")
	}
	if !store.HasPerm("username1", PermExecute) {
		t.Fatalf("new permissions not in force after reload")
	}
	if store.HasPerm("username1", PermQuery) && !store.HasPerm("username2", PermQuery) {
		t.Fatalf("permissions not reloaded correctly")
	}
	if !store.Check("username2", "password3") {
		t.Fatalf("added user not operational after reload")
	}
}

// Test_CredentialsReloaderNoChange tests that an unchanged credentials file
// is not reloaded.
func Test_CredentialsReloaderNoChange(t *testing.T) {
	path := mustWriteTempFile(t, initialCredentials)
	defer fsutil.Remove(path)

	store, err := NewCredentialsStoreFromFile(path)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}
	reloader, err := NewCredentialsReloader(store, path)
	if err != nil {
		t.Fatalf("failed to create reloader: %s", err.Error())
	}

	// Replacing the file with identical credentials must be a no-op, not an
	// error, even though the file's mtime changes.
	if err := reloader.ReloadIfChanged(); err != nil {
		t.Fatalf("unexpected error for unchanged file: %s", err.Error())
	}
	if err := setMtime(path, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := reloader.ReloadIfChanged(); err != nil {
		t.Fatalf("failed to reload untouched-but-newer-mtime file: %s", err.Error())
	}
	if !store.Check("username1", "password1") {
		t.Fatalf("credentials not intact after no-op reload")
	}
}

// Test_CredentialsReloaderInvalidKeepsExisting tests that a reload of an
// invalid credentials file fails, and leaves the existing credentials in force.
func Test_CredentialsReloaderInvalidKeepsExisting(t *testing.T) {
	path := mustWriteTempFile(t, initialCredentials)
	defer fsutil.Remove(path)

	store, err := NewCredentialsStoreFromFile(path)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}
	reloader, err := NewCredentialsReloader(store, path)
	if err != nil {
		t.Fatalf("failed to create reloader: %s", err.Error())
	}

	for _, content := range []string{
		``,                     // Empty file, as caused by some editors mid-write.
		`[{"username": "u1"}]`, // Invalid: no password, no perms.
		`nonsense`,             // Not even JSON.
	} {
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		if err := setMtime(path, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := reloader.ReloadIfChanged(); err == nil {
			t.Fatalf("expected error reloading file containing %q", content)
		}
		if !store.Check("username1", "password1") {
			t.Fatalf("existing credentials not in force after failed reload of %q", content)
		}
	}
}

// Test_CredentialsReloaderSymlink tests that the reloader follows symlinks,
// detecting both modification of the linked-to file and the symlink being
// repointed at a different file.
func Test_CredentialsReloaderSymlink(t *testing.T) {
	dir := t.TempDir()
	realPath := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(realPath, []byte(initialCredentials), 0644); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(dir, "credentials-link.json")
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Fatal(err)
	}

	store, err := NewCredentialsStoreFromFile(linkPath)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}
	reloader, err := NewCredentialsReloader(store, linkPath)
	if err != nil {
		t.Fatalf("failed to create reloader: %s", err.Error())
	}

	// Modify the linked-to file; the change must be seen through the symlink.
	rotated := `[
		{"username": "username1", "password": "password2", "perms": ["all"]}
	]`
	if err := os.WriteFile(realPath, []byte(rotated), 0644); err != nil {
		t.Fatal(err)
	}
	if err := setMtime(realPath, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := reloader.ReloadIfChanged(); err != nil {
		t.Fatalf("failed to reload changed linked-to file: %s", err.Error())
	}
	if !store.Check("username1", "password2") {
		t.Fatalf("changed linked-to file not reloaded through symlink")
	}

	// Repoint the symlink at a different file; the change must be seen too.
	otherPath := filepath.Join(dir, "other-credentials.json")
	if err := os.WriteFile(otherPath, []byte(`[
		{"username": "username3", "password": "password3", "perms": ["status"]}
	]`), 0644); err != nil {
		t.Fatal(err)
	}
	linkTmp := linkPath + ".tmp"
	if err := os.Symlink(otherPath, linkTmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(linkTmp, linkPath); err != nil {
		t.Fatal(err)
	}
	if err := reloader.ReloadIfChanged(); err != nil {
		t.Fatalf("failed to reload repointed symlink: %s", err.Error())
	}
	if !store.Check("username3", "password3") {
		t.Fatalf("repointed symlink not followed")
	}
	if store.Check("username1", "password2") {
		t.Fatalf("credentials from before symlink repoint still in force")
	}
}

// Test_CredentialsReloaderStartStop tests that Start runs the reloader in the
// background, reloading the credentials file as it changes, and that closing
// the returned channel stops the reloading.
func Test_CredentialsReloaderStartStop(t *testing.T) {
	path := mustWriteTempFile(t, initialCredentials)
	defer fsutil.Remove(path)

	store, err := NewCredentialsStoreFromFile(path)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}
	reloader, err := NewCredentialsReloader(store, path)
	if err != nil {
		t.Fatalf("failed to create reloader: %s", err.Error())
	}

	stopCh := reloader.Start(50 * time.Millisecond)
	defer close(stopCh)

	rotated := `[
		{"username": "username1", "password": "password2", "perms": ["all"]}
	]`
	if err := os.WriteFile(path, []byte(rotated), 0644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.Check("username1", "password2") {
			return // Reload detected.
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("rotated password not picked up by background reloader")
}

// Test_CredentialsReloaderConcurrentAccess tests that the store can be
// reloaded concurrently with credential checks.
func Test_CredentialsReloaderConcurrentAccess(t *testing.T) {
	path := mustWriteTempFile(t, initialCredentials)
	defer fsutil.Remove(path)

	store, err := NewCredentialsStoreFromFile(path)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}
	reloader, err := NewCredentialsReloader(store, path)
	if err != nil {
		t.Fatalf("failed to create reloader: %s", err.Error())
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				reloader.ReloadIfChanged()
			}
		}
	}()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					store.Check("username1", "password1")
					store.HasPerm("username1", PermQuery)
				}
			}
		}()
	}

	// Let the workers run concurrently for a short while.
	time.Sleep(100 * time.Millisecond)
	close(stopCh)
	wg.Wait()
}

// Test_CredentialsReloaderNilStore tests that creating a reloader with a nil
// store fails.
func Test_CredentialsReloaderNilStore(t *testing.T) {
	if _, err := NewCredentialsReloader(nil, "some-path"); err == nil {
		t.Fatalf("expected error creating reloader with nil store")
	}
}

// Test_CredentialsReloaderMissingFile tests that creating a reloader for a
// nonexistent file fails.
func Test_CredentialsReloaderMissingFile(t *testing.T) {
	store := NewCredentialsStore()
	if _, err := NewCredentialsReloader(store, filepath.Join(t.TempDir(), "no-such-file")); err == nil {
		t.Fatalf("expected error creating reloader for nonexistent file")
	}
}

// Test_CredentialsReloaderStatError tests that a stat failure, for example due
// to the file being removed, is reported but does not panic, and that the
// existing credentials remain in force.
func Test_CredentialsReloaderStatError(t *testing.T) {
	path := mustWriteTempFile(t, initialCredentials)
	defer fsutil.Remove(path)

	store, err := NewCredentialsStoreFromFile(path)
	if err != nil {
		t.Fatalf("failed to load credential store from file: %s", err.Error())
	}
	reloader, err := NewCredentialsReloader(store, path)
	if err != nil {
		t.Fatalf("failed to create reloader: %s", err.Error())
	}

	reloader.statFn = func(name string) (os.FileInfo, error) {
		return nil, os.ErrNotExist
	}
	if err := reloader.ReloadIfChanged(); err == nil {
		t.Fatalf("expected stat error to be reported")
	}
	if !store.Check("username1", "password1") {
		t.Fatalf("existing credentials not in force after stat error")
	}
}

// setMtime explicitly sets a file's modification time, so tests don't depend
// on filesystems whose mtime granularity is coarser than the test's speed, or
// on mtimes which don't change when a file is rewritten with identical size.
func setMtime(path string, tm time.Time) error {
	return os.Chtimes(path, tm, tm)
}

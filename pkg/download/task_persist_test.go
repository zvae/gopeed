package download

import (
	"bytes"
	"net"
	nethttp "net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/GopeedLab/gopeed/internal/fetcher"
	"github.com/GopeedLab/gopeed/pkg/base"
	"github.com/GopeedLab/gopeed/pkg/protocol/http"
)

const persistTestFileName = "persist.data"

// persistServerData is what the test server serves.
var persistServerData = bytes.Repeat([]byte("0123456789abcdef"), 32000)

// persistExistingData is the content of an already downloaded file, same size as
// persistServerData so that an accidental overwrite can't be spotted by size alone.
var persistExistingData = bytes.Repeat([]byte("ALREADY-DOWNLOADED!!"), 25600)

// slowReadSeeker throttles reads so tests can reliably pause a download mid-transfer.
type slowReadSeeker struct {
	*bytes.Reader
	chunkDelay time.Duration
}

func (s *slowReadSeeker) Read(p []byte) (int, error) {
	if s.chunkDelay > 0 {
		time.Sleep(s.chunkDelay)
		// cap the chunk size to keep the transfer duration predictable
		if len(p) > 16*1024 {
			p = p[:16*1024]
		}
	}
	return s.Reader.Read(p)
}

// startPersistTestServer serves persistServerData with range support. A non-zero
// chunkDelay throttles the transfer to chunkDelay per 16KB.
func startPersistTestServer(t *testing.T, chunkDelay time.Duration) (url string, closeFn func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &nethttp.Server{
		Handler: nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			nethttp.ServeContent(w, r, persistTestFileName, time.Unix(0, 0), &slowReadSeeker{
				Reader:     bytes.NewReader(persistServerData),
				chunkDelay: chunkDelay,
			})
		}),
	}
	go server.Serve(listener)
	return "http://" + listener.Addr().String() + "/" + persistTestFileName, func() {
		server.Close()
	}
}

// newPersistTestDownloader builds a downloader backed by a bolt db inside dir, so the
// same dir can be reopened to simulate an application restart.
func newPersistTestDownloader(t *testing.T, dir string) *Downloader {
	t.Helper()

	downloader := NewDownloader(&DownloaderConfig{
		StorageDir:      dir,
		Storage:         NewBoltStorage(dir),
		RefreshInterval: 50,
	})
	if err := downloader.Setup(); err != nil {
		t.Fatal(err)
	}
	// Close is idempotent, always register it so a t.Fatal mid-test doesn't leave the
	// bolt db open (Windows can't remove the temp dir of an open file).
	t.Cleanup(func() {
		downloader.Close()
	})
	return downloader
}

func storedTaskIds(t *testing.T, downloader *Downloader) []string {
	t.Helper()

	var tasks []*Task
	if err := downloader.storage.List(bucketTask, &tasks); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	return ids
}

// listenEvent registers the listener, it must be called before any task exists,
// Downloader.Listener writes the listener without synchronization.
func listenEvent(downloader *Downloader, key EventKey) <-chan struct{} {
	ch := make(chan struct{}, 1)
	downloader.Listener(func(event *Event) {
		if event.Key == key {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})
	return ch
}

func waitEvent(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal("timeout waiting for event")
	}
}

// TestDownloader_Extension_OnCreate_Delete checks that a task deleted from the onCreate
// event leaves nothing behind: no task in the list, no record in storage, no file on
// disk, and nothing that comes back after a restart.
func TestDownloader_Extension_OnCreate_Delete(t *testing.T) {
	url, closeServer := startPersistTestServer(t, 0)
	defer closeServer()

	dir := t.TempDir()
	downloader := newPersistTestDownloader(t, dir)

	if _, err := downloader.InstallExtensionByFolder("./testdata/extensions/on_create_delete", false); err != nil {
		t.Fatal(err)
	}

	taskId, err := downloader.CreateDirect(&base.Request{
		URL:    url,
		Labels: map[string]string{"skip": "true"},
	}, &base.Options{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	if taskId != "" {
		t.Fatalf("expect empty task id for a task deleted in onCreate, got %s", taskId)
	}
	if tasks := downloader.GetTasks(); len(tasks) != 0 {
		t.Fatalf("expect no task in memory, got %d", len(tasks))
	}
	if ids := storedTaskIds(t, downloader); len(ids) != 0 {
		t.Fatalf("expect no task in storage, got %v", ids)
	}

	// restart
	if err := downloader.Close(); err != nil {
		t.Fatal(err)
	}
	downloader = newPersistTestDownloader(t, dir)
	defer downloader.Close()

	if tasks := downloader.GetTasks(); len(tasks) != 0 {
		t.Fatalf("task deleted in onCreate resurrected after restart, got %d task(s)", len(tasks))
	}
	if _, err := os.Stat(filepath.Join(dir, persistTestFileName)); !os.IsNotExist(err) {
		t.Fatal("file was downloaded although the task was deleted in onCreate")
	}
}

// TestDownloader_DeletedTaskNotResurrectedByHandlers checks that the background
// start/pause handlers can't write a deleted task back to storage. Those handlers run
// on their own goroutines, so they can still be in flight when an extension deletes the
// task, and before the fix the late write brought the task back on the next startup.
func TestDownloader_DeletedTaskNotResurrectedByHandlers(t *testing.T) {
	url, closeServer := startPersistTestServer(t, 0)
	defer closeServer()

	dir := t.TempDir()
	downloader := newPersistTestDownloader(t, dir)
	// keep the task queued instead of downloading, this test is only about persistence
	downloader.cfg.MaxRunning = 0

	taskId, err := downloader.CreateDirect(&base.Request{URL: url}, &base.Options{
		Path: dir,
		Name: persistTestFileName,
		Extra: http.OptsExtra{
			Connections: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	task := downloader.GetTask(taskId)
	if task == nil {
		t.Fatal("task not created")
	}
	if ids := storedTaskIds(t, downloader); len(ids) != 1 {
		t.Fatalf("expect the task to be persisted on create, got %v", ids)
	}

	if err := downloader.Delete(&TaskFilter{IDs: []string{taskId}}, false); err != nil {
		t.Fatal(err)
	}

	// Simulate the writes an in-flight background handler would perform after the
	// delete already happened.
	downloader.doPause(task)
	if err := downloader.saveTask(task); err != nil {
		t.Fatal(err)
	}
	if err := downloader.putTask(task); err != nil {
		t.Fatal(err)
	}
	downloader.waitTaskHandlers()

	for _, id := range storedTaskIds(t, downloader) {
		if id == taskId {
			t.Fatal("deleted task was written back to storage")
		}
	}

	// restart
	if err := downloader.Close(); err != nil {
		t.Fatal(err)
	}
	downloader = newPersistTestDownloader(t, dir)
	defer downloader.Close()

	if downloader.GetTask(taskId) != nil {
		t.Fatal("deleted task resurrected after restart")
	}
}

// TestDownloader_RestoredTaskWithoutProgressDoesNotOverwrite checks the file safety net:
// a task restored from storage that never downloaded anything and has no persisted
// fetcher progress restarts from zero, so it must go through the duplicate check instead
// of opening — and overwriting — a file it does not own.
func TestDownloader_RestoredTaskWithoutProgressDoesNotOverwrite(t *testing.T) {
	url, closeServer := startPersistTestServer(t, 0)
	defer closeServer()

	dir := t.TempDir()
	existingFile := filepath.Join(dir, persistTestFileName)
	if err := os.WriteFile(existingFile, persistExistingData, 0644); err != nil {
		t.Fatal(err)
	}

	downloader := newPersistTestDownloader(t, dir)

	// A leftover task record pointing at the already downloaded file, with no progress
	// and no entry in the save bucket.
	const phantomId = "phantom-task"
	phantom := &Task{
		ID:       phantomId,
		Protocol: "http",
		Status:   base.DownloadStatusPause,
		Progress: &Progress{},
		Meta: &fetcher.FetcherMeta{
			Req: &base.Request{URL: url},
			Opts: &base.Options{
				Path:        dir,
				Name:        persistTestFileName,
				SelectFiles: []int{},
				Extra: http.OptsExtra{
					Connections: 1,
				},
			},
			Res: &base.Resource{
				Size:  int64(len(persistServerData)),
				Range: true,
				Files: []*base.FileInfo{
					{
						Name: persistTestFileName,
						Size: int64(len(persistServerData)),
					},
				},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := downloader.storage.Put(bucketTask, phantomId, phantom); err != nil {
		t.Fatal(err)
	}
	if err := downloader.Close(); err != nil {
		t.Fatal(err)
	}

	// restart, the leftover record comes back as a paused task
	downloader = newPersistTestDownloader(t, dir)
	defer downloader.Close()
	done := listenEvent(downloader, EventKeyDone)

	if downloader.GetTask(phantomId) == nil {
		t.Fatal("leftover task not restored")
	}

	if err := downloader.Continue(&TaskFilter{IDs: []string{phantomId}}); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, done, time.Second*30)

	got, err := os.ReadFile(existingFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, persistExistingData) {
		t.Fatalf("the already downloaded file was overwritten, size %d", len(got))
	}

	renamed := filepath.Join(dir, "persist (1).data")
	downloaded, err := os.ReadFile(renamed)
	if err != nil {
		t.Fatalf("expect the restored task to download into %s: %v", renamed, err)
	}
	if !bytes.Equal(downloaded, persistServerData) {
		t.Fatalf("downloaded content mismatch, got %d bytes want %d", len(downloaded), len(persistServerData))
	}
}

// TestDownloader_ResumeKeepsUsingItsOwnFile is the counterpart of the test above: a task
// paused mid-download owns its file (its fetcher progress is persisted in the save
// bucket), so after a restart it must resume into that file instead of being renamed —
// even when it hasn't downloaded a single byte yet.
func TestDownloader_ResumeKeepsUsingItsOwnFile(t *testing.T) {
	// Throttle the transfer (~3s for 512KB) so the download reliably outlives the
	// pause below.
	url, closeServer := startPersistTestServer(t, time.Millisecond*100)
	defer closeServer()

	dir := t.TempDir()
	downloader := newPersistTestDownloader(t, dir)
	// EventKeyStart fires after fetcher.Start + saveTask, i.e. once the task owns its
	// file and its progress is persisted.
	started := listenEvent(downloader, EventKeyStart)

	taskId, err := downloader.CreateDirect(&base.Request{URL: url}, &base.Options{
		Path: dir,
		Name: persistTestFileName,
		Extra: http.OptsExtra{
			Connections: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitEvent(t, started, time.Second*30)

	// Pause mid-download and quit.
	if err := downloader.Pause(&TaskFilter{IDs: []string{taskId}}); err != nil {
		t.Fatal(err)
	}
	if err := downloader.Close(); err != nil {
		t.Fatal(err)
	}

	// restart and continue, the task must keep using its own file
	downloader = newPersistTestDownloader(t, dir)

	task := downloader.GetTask(taskId)
	if task == nil {
		t.Fatal("task not restored")
	}
	if task.Status != base.DownloadStatusPause {
		t.Fatalf("expect status pause after restart, got %s", task.Status)
	}
	done := listenEvent(downloader, EventKeyDone)
	if err := downloader.Continue(&TaskFilter{IDs: []string{taskId}}); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, done, time.Second*30)

	if _, err := os.Stat(filepath.Join(dir, "persist (1).data")); !os.IsNotExist(err) {
		t.Fatal("a resumed task must not be renamed, it owns its file")
	}
	got, err := os.ReadFile(filepath.Join(dir, persistTestFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, persistServerData) {
		t.Fatal("downloaded file content mismatch after resume")
	}
}

// TestDownloader_RestoredTaskWithSaveDataResumesOwnFile pins the resumable flag: a
// restored task with zero progress but a persisted save-bucket record owns its file
// (it paused right after starting), so it must NOT be renamed even though it restarts
// the download from scratch.
func TestDownloader_RestoredTaskWithSaveDataResumesOwnFile(t *testing.T) {
	url, closeServer := startPersistTestServer(t, 0)
	defer closeServer()

	dir := t.TempDir()
	// The file created by the task before it was paused, preallocated garbage.
	ownedFile := filepath.Join(dir, persistTestFileName)
	if err := os.WriteFile(ownedFile, make([]byte, len(persistServerData)), 0644); err != nil {
		t.Fatal(err)
	}

	downloader := newPersistTestDownloader(t, dir)

	const pausedId = "paused-task"
	paused := &Task{
		ID:       pausedId,
		Protocol: "http",
		Status:   base.DownloadStatusPause,
		Progress: &Progress{},
		Meta: &fetcher.FetcherMeta{
			Req: &base.Request{URL: url},
			Opts: &base.Options{
				Path:        dir,
				Name:        persistTestFileName,
				SelectFiles: []int{},
				Extra: http.OptsExtra{
					Connections: 1,
				},
			},
			Res: &base.Resource{
				Size:  int64(len(persistServerData)),
				Range: true,
				Files: []*base.FileInfo{
					{
						Name: persistTestFileName,
						Size: int64(len(persistServerData)),
					},
				},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := downloader.storage.Put(bucketTask, pausedId, paused); err != nil {
		t.Fatal(err)
	}
	// The persisted fetcher state marks the task as the owner of its file, its content
	// doesn't matter here (a fresh restart re-downloads everything).
	if err := downloader.storage.Put(bucketSave, pausedId, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := downloader.Close(); err != nil {
		t.Fatal(err)
	}

	// restart and continue
	downloader = newPersistTestDownloader(t, dir)

	if downloader.GetTask(pausedId) == nil {
		t.Fatal("paused task not restored")
	}
	done := listenEvent(downloader, EventKeyDone)
	if err := downloader.Continue(&TaskFilter{IDs: []string{pausedId}}); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, done, time.Second*30)

	if _, err := os.Stat(filepath.Join(dir, "persist (1).data")); !os.IsNotExist(err) {
		t.Fatal("a task with persisted save data owns its file and must not be renamed")
	}
	got, err := os.ReadFile(ownedFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, persistServerData) {
		t.Fatalf("resumed task must complete into its own file, got %d bytes", len(got))
	}
}

// TestDownloader_GetTasksReturnsCopy makes sure callers can't observe the internal slice,
// Delete rewrites it in place.
func TestDownloader_GetTasksReturnsCopy(t *testing.T) {
	dir := t.TempDir()
	downloader := newPersistTestDownloader(t, dir)
	defer downloader.Close()

	func() {
		downloader.lock.Lock()
		defer downloader.lock.Unlock()

		for _, id := range []string{"a", "b"} {
			task := &Task{ID: id, Status: base.DownloadStatusDone, Progress: &Progress{}}
			initTask(task)
			downloader.tasks = append(downloader.tasks, task)
		}
	}()

	tasks := downloader.GetTasks()
	if len(tasks) != 2 {
		t.Fatalf("expect 2 tasks, got %d", len(tasks))
	}
	tasks[0] = nil

	downloader.lock.Lock()
	defer downloader.lock.Unlock()
	if downloader.tasks[0] == nil {
		t.Fatal("GetTasks must not expose the internal slice")
	}
}

// TestDownloader_ConcurrentCreateAndPauseAll runs creations that are cancelled by an
// extension while the downloader is repeatedly pausing everything, which is what happens
// when the user quits the app right after adding a batch of links. Nothing may end up in
// storage. Worth running with -race.
func TestDownloader_ConcurrentCreateAndPauseAll(t *testing.T) {
	url, closeServer := startPersistTestServer(t, 0)
	defer closeServer()

	dir := t.TempDir()
	downloader := newPersistTestDownloader(t, dir)

	if _, err := downloader.InstallExtensionByFolder("./testdata/extensions/on_create_delete", false); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var pauser sync.WaitGroup
	pauser.Add(1)
	go func() {
		defer pauser.Done()
		for {
			select {
			case <-stop:
				return
			default:
				downloader.pauseAll()
				downloader.GetTasks()
			}
		}
	}()

	var creators sync.WaitGroup
	for i := 0; i < 20; i++ {
		creators.Add(1)
		go func() {
			defer creators.Done()
			downloader.CreateDirect(&base.Request{
				URL:    url,
				Labels: map[string]string{"skip": "true"},
			}, &base.Options{Path: dir})
		}()
	}
	creators.Wait()
	close(stop)
	pauser.Wait()
	downloader.waitTaskHandlers()

	if tasks := downloader.GetTasks(); len(tasks) != 0 {
		t.Fatalf("expect no task in memory, got %d", len(tasks))
	}
	if ids := storedTaskIds(t, downloader); len(ids) != 0 {
		t.Fatalf("expect no task in storage, got %v", ids)
	}

	// restart
	if err := downloader.Close(); err != nil {
		t.Fatal(err)
	}
	downloader = newPersistTestDownloader(t, dir)
	defer downloader.Close()

	if tasks := downloader.GetTasks(); len(tasks) != 0 {
		t.Fatalf("deleted tasks resurrected after restart, got %d", len(tasks))
	}
}

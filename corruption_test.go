// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	stderrors "errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb/encoding"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

// TestIsBlockDirIgnoresIncompleteWrites asserts that a directory still being
// populated by a compaction (the "<ulid>.tmp" form, or any name that is not a
// valid ULID) is never considered a visible block. Only the atomically renamed
// final directory is exposed to queries.
func TestIsBlockDirIgnoresIncompleteWrites(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	complete := createBlock(t, tmpdir, genSeries(1, 1, 0, 10))

	// Simulate an in-progress compaction: a partially written ".tmp" directory
	// that must remain invisible even though it already carries some files.
	inProgress := complete + ".tmp"
	testutil.Ok(t, os.MkdirAll(inProgress, 0777))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(inProgress, "index"), []byte("partial"), 0666))

	// A non-ULID scratch directory must be ignored as well.
	testutil.Ok(t, os.MkdirAll(filepath.Join(tmpdir, "scratch"), 0777))

	dirs, err := blockDirs(tmpdir)
	testutil.Ok(t, err)
	testutil.Assert(t, len(dirs) == 1, "expected exactly one visible block, got %v", dirs)
	testutil.Equals(t, complete, dirs[0])
}

// TestClassifyBlockErr checks the permanent-vs-transparent error split.
func TestClassifyBlockErr(t *testing.T) {
	dir := filepath.Join("some", "01ARZ3NDEKTSV4RRFFQ69G5FAV")

	// Structural errors are permanent corruption.
	corrupt := classifyBlockErr(dir, CorruptIndex, encoding.ErrInvalidSize)
	testutil.Assert(t, IsCorruptionErr(corrupt), "expected corruption error, got %v", corrupt)
	testutil.Assert(t, !IsTemporaryErr(corrupt), "must not be classified temporary")

	corruptChunks := classifyBlockErr(dir, CorruptChunks, errChunkHeaderSentinel())
	testutil.Assert(t, IsCorruptionErr(corruptChunks), "chunk truncation must be corruption")

	// A missing file during an atomic swap is transient.
	temp := classifyBlockErr(dir, CorruptIndex, os.ErrNotExist)
	testutil.Assert(t, IsTemporaryErr(temp), "expected temporary error, got %v", temp)
	testutil.Assert(t, !IsCorruptionErr(temp), "must not be classified corruption")

	// Nil passes through untouched.
	testutil.Assert(t, classifyBlockErr(dir, CorruptIndex, nil) == nil, "nil error must stay nil")
}

func errChunkHeaderSentinel() error {
	return encoding.ErrInvalidChecksum
}

// TestOpenBlockDistinguishesCorruptionFromTemporary exercises the full open
// path: a missing file is temporary, a truncated index is corruption.
func TestOpenBlockDistinguishesCorruptionFromTemporary(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	blockDir := createBlock(t, tmpdir, genSeries(3, 2, 0, 100))

	// The healthy block opens cleanly.
	healthy, err := OpenBlock(nil, blockDir, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, healthy.Close())

	// Remove the index: the block directory exists but a file is briefly gone,
	// which is the shape of an interrupted/in-progress swap -> temporary.
	indexPath := filepath.Join(blockDir, indexFilename)
	testutil.Ok(t, os.Remove(indexPath))
	_, err = OpenBlock(nil, blockDir, nil)
	testutil.NotOk(t, err)
	testutil.Assert(t, IsTemporaryErr(err), "missing index should be temporary, got %v", err)

	// Recreate the block and truncate the index mid-file -> permanent damage.
	testutil.Ok(t, os.RemoveAll(tmpdir))
	testutil.Ok(t, os.MkdirAll(tmpdir, 0777))
	blockDir = createBlock(t, tmpdir, genSeries(3, 2, 0, 100))
	indexPath = filepath.Join(blockDir, indexFilename)

	info, err := os.Stat(indexPath)
	testutil.Ok(t, err)
	testutil.Assert(t, info.Size() > index.HeaderLen, "test setup needs an index larger than the header")
	truncated := make([]byte, index.HeaderLen+1)
	original, err := ioutil.ReadFile(indexPath)
	testutil.Ok(t, err)
	copy(truncated, original)
	testutil.Ok(t, ioutil.WriteFile(indexPath, truncated, info.Mode()))

	_, err = OpenBlock(nil, blockDir, nil)
	testutil.NotOk(t, err)
	testutil.Assert(t, IsCorruptionErr(err), "truncated index must be reported as corruption, got %v", err)
}

// TestCompactionInvisibleUntilComplete runs many compactions while a query
// goroutine hammers the DB. Incomplete blocks must never surface: every query
// either reads the previous block set or the fully written new one, and no
// error is allowed to escape for the whole compaction window.
func TestCompactionInvisibleUntilComplete(t *testing.T) {
	const (
		iterations   = 20
		queryWorkers = 4
	)

	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	opts := DefaultOptions
	// Short ranges so head blocks become compactable quickly.
	opts.BlockRanges = ExponentialBlockRanges(1000, 3, 3)

	db, err := Open(tmpdir, log.NewNopLogger(), nil, opts)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, db.Close()) }()

	label := labels.FromStrings("job", "guard")

	var (
		wg          sync.WaitGroup
		stop        = make(chan struct{})
		queryErrors = make(chan error, queryWorkers*256)
		queries     int64
		qmu         sync.Mutex
	)

	// Continuous readers for the entire compaction window.
	for w := 0; w < queryWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				q, qerr := db.Querier(0, 1<<62)
				if qerr != nil {
					queryErrors <- qerr
					continue
				}
				if _, qerr = q.Select(labels.NewEqualMatcher("job", "guard")); qerr != nil {
					queryErrors <- qerr
				}
				if cerr := q.Close(); cerr != nil {
					queryErrors <- cerr
				}
				qmu.Lock()
				queries++
				qmu.Unlock()
			}
		}()
	}

	// Drive repeated head persistence + leveled compactions.
	for i := 0; i < iterations; i++ {
		base := int64(i) * 4000
		app := db.Appender()
		for ts := base; ts < base+4000; ts += 10 {
			_, err = app.Add(label, ts, float64(ts))
			testutil.Ok(t, err)
		}
		testutil.Ok(t, app.Commit())
		testutil.Ok(t, db.compact())

		// While compaction machinery may still be settling, the on-disk layout
		// must never expose a half-written directory.
		dirs, derr := blockDirs(db.Dir())
		testutil.Ok(t, derr)
		for _, d := range dirs {
			_, _, merr := readMetaFile(d)
			testutil.Ok(t, merr)
		}
	}

	close(stop)
	wg.Wait()
	close(queryErrors)

	qmu.Lock()
	totalQueries := queries
	qmu.Unlock()
	testutil.Assert(t, totalQueries > 0, "expected concurrent queries to run")

	for qerr := range queryErrors {
		t.Fatalf("query failed during compaction window: %v", qerr)
	}
	t.Logf("compaction window: %d iterations, %d concurrent queries with zero errors", iterations, totalQueries)
}

// TestTruncatedBlockReportedAsCorruption is the negative counterpart. After a
// block is fully published we deliberately truncate a chunk file on disk. The
// next reload/query must fail with a corruption error that names the damaged
// block, rather than a generic or temporary error.
func TestTruncatedBlockReportedAsCorruption(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	blockDir := createBlock(t, tmpdir, genSeries(5, 2, 0, 100))

	chunkFiles, err := sequenceFiles(filepath.Join(blockDir, "chunks"))
	testutil.Ok(t, err)
	testutil.Assert(t, len(chunkFiles) > 0, "expected at least one chunk segment")

	// Truncate the chunk segment to less than its header size.
	target := chunkFiles[0]
	info, err := os.Stat(target)
	testutil.Ok(t, err)
	testutil.Assert(t, info.Size() > 4, "test setup needs a non-trivial chunk file")
	testutil.Ok(t, os.Truncate(target, 2))

	_, err = OpenBlock(nil, blockDir, nil)
	testutil.NotOk(t, err)
	testutil.Assert(t, IsCorruptionErr(err), "truncated chunk must surface as corruption, got %v", err)

	var corrupted *ErrBlockCorrupted
	testutil.Assert(t, stderrors.As(err, &corrupted), "error should carry block identity")
	testutil.Equals(t, CorruptChunks, corrupted.Type)
	testutil.Equals(t, blockDir, corrupted.BlockDir)
	t.Logf("corruption correctly detected: %v", corrupted)

	// And the same classification propagates through a full DB reload.
	db, cleanup := openTestDB(t, nil)
	testutil.Ok(t, db.Close())
	cleanup()

	// Open a fresh DB directly on the directory containing the damaged block.
	reopened, err := Open(tmpdir, log.NewNopLogger(), nil, nil)
	if err == nil {
		defer func() { testutil.Ok(t, reopened.Close()) }()
		err = reopened.reload()
	}
	testutil.NotOk(t, err)
	testutil.Assert(t, IsCorruptionErr(err), "reload over a truncated block must be corruption, got %v", err)
}

// TestTemporaryBlockDoesNotEvictOldBlocks ensures a transient open failure for
// a freshly appearing block keeps serving the previously loaded blocks instead
// of failing the whole reload.
func TestTemporaryBlockDoesNotEvictOldBlocks(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpdir)) }()

	first := createBlock(t, tmpdir, genSeries(2, 1, 0, 50))

	db, cleanup := openTestDBAt(t, tmpdir)
	defer func() { testutil.Ok(t, db.Close()); cleanup() }()

	testutil.Assert(t, len(db.Blocks()) == 1, "expected one loaded block")

	// Publish a second block directory whose index is momentarily missing,
	// mimicking a swap observed at an unlucky instant.
	second := createBlock(t, tmpdir, genSeries(2, 1, 60, 110))
	testutil.Ok(t, os.Remove(filepath.Join(second, indexFilename)))

	testutil.Ok(t, db.reload())
	testutil.Assert(t, len(db.Blocks()) == 1, "old block must remain visible during transient failure")
	testutil.Equals(t, filepath.Base(first), db.Blocks()[0].Meta().ULID.String())

	// Restore the file; the next reload picks the block up without error.
	restoreBlockIndex(t, second)
	testutil.Ok(t, db.reload())
	testutil.Assert(t, len(db.Blocks()) == 2, "block must become visible once complete")
}

func openTestDBAt(t testing.TB, dir string) (*DB, func()) {
	db, err := Open(dir, log.NewNopLogger(), nil, nil)
	testutil.Ok(t, err)
	return db, func() {}
}

func restoreBlockIndex(t testing.TB, blockDir string) {
	// Recreate a valid block from scratch in a sibling directory then move its
	// index into place, keeping the same directory name.
	parent := filepath.Dir(blockDir)
	staging, err := ioutil.TempDir(parent, "restore")
	testutil.Ok(t, err)

	sibling := createBlock(t, staging, genSeries(2, 1, 60, 110))
	testutil.Ok(t, os.Rename(filepath.Join(sibling, indexFilename), filepath.Join(blockDir, indexFilename)))
	testutil.Ok(t, os.RemoveAll(staging))
}

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
	"io/ioutil"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

// benchDB builds a database with a fixed amount of data spread across many
// series so that both head compaction and range queries do measurable work.
func benchDB(b *testing.B) (*DB, func(), labels.Matcher) {
	tmpdir, err := ioutil.TempDir("", "bench")
	testutil.Ok(b, err)

	opts := DefaultOptions
	opts.BlockRanges = ExponentialBlockRanges(1000, 3, 3)

	db, err := Open(tmpdir, log.NewNopLogger(), nil, opts)
	testutil.Ok(b, err)

	const seriesCount, samplesPerSeries = 2000, 200
	app := db.Appender()
	for s := 0; s < seriesCount; s++ {
		lset := labels.FromStrings("job", "bench", "series", itoa(s))
		for i := 0; i < samplesPerSeries; i++ {
			ts := int64(i * 5)
			_, err = app.Add(lset, ts, float64(s)+float64(i)/10)
			testutil.Ok(b, err)
		}
	}
	testutil.Ok(b, app.Commit())

	cleanup := func() {
		testutil.Ok(b, db.Close())
		testutil.Ok(b, os.RemoveAll(tmpdir))
	}
	return db, cleanup, labels.NewEqualMatcher("job", "bench")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// BenchmarkProtectedCompact measures a head compaction that goes through the
// full visibility-guarded write + reload path. Every iteration ingests a fresh
// compactable window first so a real block is written and reloaded each time.
func BenchmarkProtectedCompact(b *testing.B) {
	db, cleanup, _ := benchDB(b)
	defer cleanup()

	lset := labels.FromStrings("job", "bench", "series", "compact-feed")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := int64(i+1) * 4000
		app := db.Appender()
		for ts := base; ts < base+4000; ts += 5 {
			_, err := app.Add(lset, ts, float64(ts))
			testutil.Ok(b, err)
		}
		testutil.Ok(b, app.Commit())
		testutil.Ok(b, db.compact())
	}
}

// BenchmarkProtectedQuery measures a full range query that opens block
// queriers and expands every matched series, exercising the same reader path
// the concurrency guard protects.
func BenchmarkProtectedQuery(b *testing.B) {
	db, cleanup, matcher := benchDB(b)
	defer cleanup()
	testutil.Ok(b, db.compact())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q, err := db.Querier(0, 1<<62)
		testutil.Ok(b, err)
		ss, err := q.Select(matcher)
		testutil.Ok(b, err)
		var scanned int
		for ss.Next() {
			it := ss.At().Iterator()
			for it.Next() {
				it.At()
				scanned++
			}
			testutil.Ok(b, it.Err())
		}
		testutil.Ok(b, ss.Err())
		testutil.Ok(b, q.Close())
		if scanned == 0 {
			b.Fatal("query returned no samples")
		}
	}
}

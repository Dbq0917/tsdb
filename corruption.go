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
	"io"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/encoding"
)

// CorruptionType identifies which on-disk structure of a block is corrupted.
type CorruptionType int

const (
	// CorruptMeta means the block's meta.json cannot be parsed or is invalid.
	CorruptMeta CorruptionType = iota
	// CorruptIndex means the block's index file is structurally invalid.
	CorruptIndex
	// CorruptChunks means the block's chunk files are structurally invalid.
	CorruptChunks
	// CorruptTombstones means the block's tombstone file is structurally invalid.
	CorruptTombstones
)

func (t CorruptionType) String() string {
	switch t {
	case CorruptMeta:
		return "meta"
	case CorruptIndex:
		return "index"
	case CorruptChunks:
		return "chunks"
	case CorruptTombstones:
		return "tombstones"
	default:
		return "unknown"
	}
}

// ErrBlockCorrupted marks a block whose on-disk data is structurally invalid.
// This is a permanent condition: retrying the read will keep failing until the
// block is repaired or removed. Callers must surface it rather than silently
// serving incomplete query results.
type ErrBlockCorrupted struct {
	BlockDir string
	Type     CorruptionType
	err      error
}

func (e *ErrBlockCorrupted) Error() string {
	return "corrupt block " + e.BlockDir + " (" + e.Type.String() + "): " + e.err.Error()
}

// Cause returns the underlying error that triggered the corruption detection.
func (e *ErrBlockCorrupted) Cause() error { return e.err }

// Unwrap allows errors.Is/As to reach the wrapped sentinel error.
func (e *ErrBlockCorrupted) Unwrap() error { return e.err }

// ErrBlockTemporary marks a transient failure while opening or reading a block
// that does not imply the on-disk data is damaged (e.g. the directory is being
// replaced, a file is briefly missing or an I/O operation is interrupted).
// The operation is safe to retry; a later read may succeed without any change
// to the data.
type ErrBlockTemporary struct {
	BlockDir string
	err      error
}

func (e *ErrBlockTemporary) Error() string {
	return "temporary error reading block " + e.BlockDir + ": " + e.err.Error()
}

// Cause returns the underlying error that triggered the temporary failure.
func (e *ErrBlockTemporary) Cause() error { return e.err }

// Unwrap allows errors.Is/As to reach the wrapped sentinel error.
func (e *ErrBlockTemporary) Unwrap() error { return e.err }

// IsCorruptionErr reports whether err represents a permanently corrupted block.
func IsCorruptionErr(err error) bool {
	var c *ErrBlockCorrupted
	return stderrors.As(err, &c)
}

// IsTemporaryErr reports whether err represents a transient block read failure
// that is safe to retry.
func IsTemporaryErr(err error) bool {
	var t *ErrBlockTemporary
	return stderrors.As(err, &t)
}

// classifyBlockErr separates permanent block corruption from transient
// failures. Structural format errors are wrapped as ErrBlockCorrupted of the
// given type; everything else (e.g. files temporarily missing during an
// atomic directory swap) is wrapped as ErrBlockTemporary so callers can retry.
func classifyBlockErr(blockDir string, typ CorruptionType, err error) error {
	if err == nil {
		return nil
	}
	// This repository wraps errors with github.com/pkg/errors, which exposes
	// the root cause through Cause() rather than Unwrap(). Resolve the cause
	// before matching the structural sentinel errors.
	switch cause := errors.Cause(err); cause {
	case encoding.ErrInvalidSize,
		encoding.ErrInvalidChecksum,
		chunks.ErrInvalidSize,
		io.ErrUnexpectedEOF,
		io.EOF:
		return &ErrBlockCorrupted{BlockDir: blockDir, Type: typ, err: err}
	}
	// Format violations (bad magic number, unknown version, references that
	// point outside the file) are produced from the persisted bytes alone and
	// therefore prove the block is structurally damaged. They are reported as
	// free-form errors, so match on their stable message prefixes.
	msg := err.Error()
	for _, marker := range structuralErrorMarkers {
		if strings.Contains(msg, marker) {
			return &ErrBlockCorrupted{BlockDir: blockDir, Type: typ, err: err}
		}
	}
	return &ErrBlockTemporary{BlockDir: blockDir, err: err}
}

// structuralErrorMarkers lists stable substrings of errors that can only arise
// when a block's persisted bytes violate the on-disk format, i.e. permanent
// corruption rather than a transient read failure.
var structuralErrorMarkers = []string{
	"invalid magic number",
	"invalid chunk format version",
	"unknown index file version",
	"out of range",
	"beyond data size",
	"reading chunk length failed",
}

// wrapCorrupted forces a corruption classification regardless of the wrapped
// error type. It is used where the failure is known to be structural (e.g. an
// unparseable meta.json).
func wrapCorrupted(blockDir string, typ CorruptionType, err error) error {
	if err == nil {
		return nil
	}
	return &ErrBlockCorrupted{BlockDir: blockDir, Type: typ, err: errors.WithStack(err)}
}

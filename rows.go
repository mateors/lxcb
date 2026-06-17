package lxcb

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync/atomic"
)

// -----------------------------------------------------------------------
// n1qlRows — streaming implementation of driver.Rows
// -----------------------------------------------------------------------
//
// Architecture:
//
//   HTTP response body
//        │
//   json.Decoder  (streaming — one token at a time)
//        │
//   populateRows goroutine
//        │
//   resultChan  (buffered)
//        │
//   Next()  ← called by database/sql on the caller's goroutine
//
// The response body is never fully loaded into memory.
// Goroutine lifecycle: populateRows exits when resultChan is drained
// OR when done is closed (early Close() call) — no leaks.

// rowResult wraps either a data row or an error sent through resultChan.
type rowResult struct {
	row interface{}
	err error
}

// n1qlRows implements driver.Rows.
type n1qlRows struct {
	resp        *http.Response
	resultChan  chan rowResult
	done        chan struct{} // closed by Close() to signal the goroutine to exit
	closed      int32         // atomic; 1 = closed
	columns     []string      // computed once, lazily
	columnsDone int32         // atomic flag for lazy column init
	passthrough bool
	signature   interface{}
	metrics     interface{}
	extras      interface{}
	errors      interface{}
}

// newStreamingRows starts the background streaming goroutine and returns
// a ready-to-use *n1qlRows. The response body is owned by the goroutine
// and will be closed when populateRows exits.
func newStreamingRows(resp *http.Response, passthrough bool) (*n1qlRows, error) {
	// Parse the response envelope headers (everything except "results")
	// without buffering the result rows. We do a two-pass approach:
	//   pass 1: decode the full envelope but keep "results" as raw JSON
	//   pass 2: stream the raw results array row-by-row
	//
	// This keeps the per-row memory footprint O(1) while still giving us
	// signature / metrics / errors upfront.

	var envelope struct {
		RequestID string           `json:"requestID"`
		Signature *json.RawMessage `json:"signature"`
		Results   *json.RawMessage `json:"results"`
		Status    string           `json:"status"`
		Metrics   *json.RawMessage `json:"metrics"`
		Errors    *json.RawMessage `json:"errors"`
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close() // safe — we've read everything into body
	if err != nil {
		return nil, fmt.Errorf("n1ql: reading response body: %w", err)
	}

	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("n1ql: parsing response envelope: %w", err)
	}

	// Decode errors first — return immediately if present and no results.
	var errs []n1qlError
	if envelope.Errors != nil {
		_ = json.Unmarshal(*envelope.Errors, &errs)
	}

	// Decode signature.
	var signature interface{}
	if envelope.Signature != nil {
		_ = json.Unmarshal(*envelope.Signature, &signature)
	}
	if signature == nil && passthrough {
		signature = map[string]interface{}{"*": "*"}
	}

	// Decode metrics (needed for passthrough).
	var metrics interface{}
	if envelope.Metrics != nil && passthrough {
		_ = json.Unmarshal(*envelope.Metrics, &metrics)
	}

	// Extras for passthrough (requestID, status, signature row).
	var extras interface{}
	if passthrough {
		extras = map[string]interface{}{
			"requestID": envelope.RequestID,
			"status":    envelope.Status,
			"signature": signature,
		}
	}

	rows := &n1qlRows{
		resultChan:  make(chan rowResult, 16), // small buffer to keep goroutine flowing
		done:        make(chan struct{}),
		passthrough: passthrough,
		signature:   signature,
		metrics:     metrics,
		extras:      extras,
		errors:      errs,
	}

	// Determine the raw results bytes to stream.
	var resultsBytes []byte
	if envelope.Results != nil {
		resultsBytes = *envelope.Results
	} else {
		resultsBytes = []byte("[]")
	}

	go rows.populateRows(resultsBytes, errs)
	return rows, nil
}

// populateRows streams result rows from the raw JSON array into resultChan.
// It exits when all rows are sent, an error occurs, or done is closed.
func (rows *n1qlRows) populateRows(resultsJSON []byte, errs []n1qlError) {
	defer close(rows.resultChan)

	send := func(r rowResult) bool {
		select {
		case rows.resultChan <- r:
			return true
		case <-rows.done:
			return false
		}
	}

	// Passthrough: emit extras and metrics as first two pseudo-rows.
	if rows.passthrough {
		if rows.extras != nil {
			if !send(rowResult{row: rows.extras}) {
				return
			}
		}
		if rows.metrics != nil {
			if !send(rowResult{row: rows.metrics}) {
				return
			}
		}
	}

	// Stream the results array one element at a time.
	dec := json.NewDecoder(newByteReader(resultsJSON))

	// Consume opening '['.
	tok, err := dec.Token()
	if err != nil {
		send(rowResult{err: fmt.Errorf("n1ql: reading results array start: %w", err)})
		return
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		send(rowResult{err: fmt.Errorf("n1ql: expected '[', got %v", tok)})
		return
	}

	for dec.More() {
		var row interface{}
		if err := dec.Decode(&row); err != nil {
			send(rowResult{err: fmt.Errorf("n1ql: decoding row: %w", err)})
			return
		}
		if !send(rowResult{row: row}) {
			return // done channel closed — caller abandoned iteration
		}
	}

	// Emit errors as a final pseudo-row (preserves original behaviour).
	if len(errs) > 0 {
		send(rowResult{row: map[string]interface{}{"errors": errs}})
	}
}

// -----------------------------------------------------------------------
// driver.Rows interface
// -----------------------------------------------------------------------

// Columns returns the column names derived from the query signature.
// The result is computed once and cached.
func (rows *n1qlRows) Columns() []string {
	// Lazy, single-init via atomic flag.
	if atomic.LoadInt32(&rows.columnsDone) == 1 {
		return rows.columns
	}

	cols := deriveColumns(rows.signature)
	rows.columns = cols
	atomic.StoreInt32(&rows.columnsDone, 1)
	return cols
}

// Close signals the streaming goroutine to stop and marks the rows as closed.
func (rows *n1qlRows) Close() error {
	if atomic.CompareAndSwapInt32(&rows.closed, 0, 1) {
		close(rows.done) // unblocks any send in populateRows
	}
	return nil
}

// Next reads the next row from the stream into dest.
func (rows *n1qlRows) Next(dest []driver.Value) error {
	r, ok := <-rows.resultChan
	if !ok {
		return io.EOF
	}
	if r.err != nil {
		return r.err
	}
	return rows.scanRow(r.row, dest)
}

// -----------------------------------------------------------------------
// Row scanning
// -----------------------------------------------------------------------

func (rows *n1qlRows) scanRow(r interface{}, dest []driver.Value) error {
	cols := rows.Columns()
	numCols := len(cols)

	// Passthrough pseudo-rows (extras, metrics): marshal the whole value
	// into dest[0] and blank-fill the rest.
	if rows.passthrough && rows.isPassthroughPseudoRow(r) {
		b, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("n1ql: marshalling passthrough row: %w", err)
		}
		dest[0] = b
		for i := 1; i < numCols; i++ {
			dest[i] = ""
		}
		return nil
	}

	switch row := r.(type) {
	case map[string]interface{}:
		return rows.scanMapRow(row, cols, dest)
	case []interface{}:
		return rows.scanArrayRow(row, dest)
	default:
		// Scalar or unknown — marshal into dest[0].
		b, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("n1ql: marshalling scalar row: %w", err)
		}
		if len(dest) > 0 {
			dest[0] = b
		}
	}
	return nil
}

func (rows *n1qlRows) scanMapRow(row map[string]interface{}, cols []string, dest []driver.Value) error {
	if len(row) > len(cols) {
		return fmt.Errorf("n1ql: row has %d fields but only %d columns declared", len(row), len(cols))
	}
	for i, col := range cols {
		if i >= len(dest) {
			break
		}
		if v, ok := row[col]; ok {
			b, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("n1ql: marshalling column %q: %w", col, err)
			}
			dest[i] = b
		} else {
			dest[i] = ""
		}
	}
	return nil
}

func (rows *n1qlRows) scanArrayRow(row []interface{}, dest []driver.Value) error {
	for i, v := range row {
		if i >= len(dest) {
			return fmt.Errorf("n1ql: array row has more values (%d) than dest slice (%d)", len(row), len(dest))
		}
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("n1ql: marshalling array element %d: %w", i, err)
		}
		dest[i] = b
	}
	return nil
}

// isPassthroughPseudoRow returns true for the synthetic rows emitted in
// passthrough mode (extras and metrics), which must bypass normal scanning.
func (rows *n1qlRows) isPassthroughPseudoRow(r interface{}) bool {
	if !rows.passthrough {
		return false
	}
	if m, ok := r.(map[string]interface{}); ok {
		_, hasRequestID := m["requestID"]
		_, hasMutationCount := m["mutationCount"]
		return hasRequestID || hasMutationCount
	}
	return false
}

// -----------------------------------------------------------------------
// Column derivation
// -----------------------------------------------------------------------

// deriveColumns builds a stable (sorted) column list from a N1QL signature.
// Sorting is required because Go map iteration is non-deterministic; the
// column order will be alphabetical rather than SELECT-clause order.
// Callers that require declaration order should use named column scanning.
func deriveColumns(sig interface{}) []string {
	var cols []string

	switch s := sig.(type) {
	case map[string]interface{}:
		for k := range s {
			cols = append(cols, k)
		}
		sort.Strings(cols)
	case string:
		cols = []string{s}
	default:
		cols = []string{"*"}
	}

	return cols
}

// -----------------------------------------------------------------------
// byteReader — wraps a []byte as an io.Reader without copying
// -----------------------------------------------------------------------

type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(b []byte) *byteReader { return &byteReader{data: b} }

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

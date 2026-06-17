// Package lxcb implements a database/sql driver for Couchbase N1QL.
// It registers under the driver name "n1ql".
//
// Usage:
//
//	db, err := sql.Open("n1ql", "http://localhost:8093")
package lxcb

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sync"
)

// -----------------------------------------------------------------------
// Package-level defaults
// -----------------------------------------------------------------------

var (
	// DefaultMaxIdleConnsPerHost controls the connection pool size for the
	// default HTTP transport. Must be set before calling sql.Open.
	DefaultMaxIdleConnsPerHost = 10

	// DefaultHTTPClient is used when no custom client is provided via Config.
	DefaultHTTPClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: DefaultMaxIdleConnsPerHost,
		},
	}

	// placeholderRe matches bare ? placeholders in a query.
	placeholderRe = regexp.MustCompile(`\?`)
)

func init() {
	sql.Register("n1ql", &n1qlDriver{})
}

// -----------------------------------------------------------------------
// Driver
// -----------------------------------------------------------------------

// n1qlDriver implements driver.Driver.
type n1qlDriver struct{}

// Open opens a new connection to the N1QL query service.
// dataSourceName must be the base URL of the Couchbase node,
// e.g. "http://localhost:8093".
func (d *n1qlDriver) Open(dataSourceName string) (driver.Conn, error) {
	return openConn(context.Background(), dataSourceName, nil)
}

// -----------------------------------------------------------------------
// Config — per-connection settings (no globals)
// -----------------------------------------------------------------------

// Config holds per-connection query parameters and optional HTTP client
// override. Pass nil to use package defaults.
type Config struct {
	// QueryParams are appended to every N1QL request (e.g. "scan_consistency").
	QueryParams map[string]string

	// HTTPClient overrides DefaultHTTPClient when set.
	HTTPClient *http.Client

	// PassthroughMode causes metrics/status/requestID to be returned as
	// extra rows, matching the raw N1QL response envelope.
	PassthroughMode bool
}

// -----------------------------------------------------------------------
// Connection
// -----------------------------------------------------------------------

// n1qlConn implements driver.Conn, driver.QueryerContext, driver.ExecerContext.
type n1qlConn struct {
	queryAPI string
	client   *http.Client
	config   Config
	mu       sync.RWMutex // protects queryAPI only; config is read-only after construction
}

func openConn(ctx context.Context, dataSourceName string, cfg *Config) (*n1qlConn, error) {
	c := &n1qlConn{
		queryAPI: fmt.Sprintf("%s/query/service", dataSourceName),
		client:   DefaultHTTPClient,
	}

	if cfg != nil {
		c.config = *cfg
		if cfg.HTTPClient != nil {
			c.client = cfg.HTTPClient
		}
	}

	// Validate connectivity.
	req, err := c.buildRequest(ctx, "SELECT 1", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("n1ql: connection failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("n1ql: connection check returned HTTP %d: %s", resp.StatusCode, body)
	}

	return c, nil
}

// Close is a no-op; the HTTP client manages its own connection pool.
func (c *n1qlConn) Close() error { return nil }

// Begin is not supported — Couchbase N1QL does not expose SQL transactions
// via this interface.
func (c *n1qlConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("n1ql: transactions are not supported")
}

// -----------------------------------------------------------------------
// Prepare
// -----------------------------------------------------------------------

// Prepare sends a PREPARE statement to Couchbase and returns a driver.Stmt.
func (c *n1qlConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext is the context-aware variant of Prepare.
func (c *n1qlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prepared, argCount := replacePlaceholders(query)
	prepareSQL := "PREPARE " + prepared

	req, err := c.buildRequest(ctx, prepareSQL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("n1ql: prepare request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("n1ql: prepare returned HTTP %d: %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("n1ql: reading prepare response: %w", err)
	}

	var envelope struct {
		Errors  []n1qlError       `json:"errors"`
		Results []json.RawMessage `json:"results"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("n1ql: parsing prepare response: %w", err)
	}

	if len(envelope.Errors) > 0 {
		return nil, fmt.Errorf("n1ql: prepare error: %s", serializeErrors(envelope.Errors))
	}

	if len(envelope.Results) == 0 {
		return nil, fmt.Errorf("n1ql: prepare returned no results")
	}

	var meta struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(envelope.Results[0], &meta); err != nil {
		return nil, fmt.Errorf("n1ql: parsing prepared statement metadata: %w", err)
	}

	return &n1qlStmt{
		conn:     c,
		prepared: string(envelope.Results[0]),
		name:     meta.Name,
		argCount: argCount,
	}, nil
}

// -----------------------------------------------------------------------
// Query / Exec (direct, non-prepared)
// -----------------------------------------------------------------------

// QueryContext implements driver.QueryerContext.
func (c *n1qlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return c.execQuery(ctx, query, vals)
}

// ExecContext implements driver.ExecerContext.
func (c *n1qlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return c.execMutation(ctx, query, vals)
}

// Legacy driver.Queryer / driver.Execer (no context).
func (c *n1qlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.execQuery(context.Background(), query, args)
}

func (c *n1qlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.execMutation(context.Background(), query, args)
}

// -----------------------------------------------------------------------
// Internal query execution
// -----------------------------------------------------------------------

func (c *n1qlConn) execQuery(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	req, err := c.buildRequestWithArgs(ctx, query, args, nil)
	if err != nil {
		return nil, err
	}
	return c.doQuery(req)
}

func (c *n1qlConn) execMutation(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	req, err := c.buildRequestWithArgs(ctx, query, args, nil)
	if err != nil {
		return nil, err
	}
	return c.doExec(req)
}

// doQuery fires the HTTP request and returns a streaming Rows.
func (c *n1qlConn) doQuery(req *http.Request) (driver.Rows, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("n1ql: query request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("n1ql: query returned HTTP %d: %s", resp.StatusCode, body)
	}

	return newStreamingRows(resp, c.config.PassthroughMode)
}

// doExec fires the HTTP request and parses mutation metrics.
func (c *n1qlConn) doExec(req *http.Request) (driver.Result, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("n1ql: exec request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("n1ql: exec returned HTTP %d: %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("n1ql: reading exec response: %w", err)
	}

	var envelope struct {
		Errors  []n1qlError            `json:"errors"`
		Metrics map[string]interface{} `json:"metrics"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("n1ql: parsing exec response: %w", err)
	}

	if len(envelope.Errors) > 0 {
		return nil, fmt.Errorf("n1ql: exec error: %s", serializeErrors(envelope.Errors))
	}

	res := &n1qlResult{}
	if mc, ok := envelope.Metrics["mutationCount"]; ok {
		if f, ok := mc.(float64); ok {
			res.affectedRows = int64(f)
		}
	}
	return res, nil
}

// -----------------------------------------------------------------------
// Request builders
// -----------------------------------------------------------------------

// buildRequest constructs a plain POST request with a raw query string.
func (c *n1qlConn) buildRequest(ctx context.Context, query string, extraParams map[string]string) (*http.Request, error) {
	c.mu.RLock()
	api := c.queryAPI
	c.mu.RUnlock()

	form := url.Values{}
	form.Set("statement", query)
	c.applyQueryParams(&form)
	for k, v := range extraParams {
		form.Set(k, v)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("n1ql: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req, nil
}

// buildRequestWithArgs handles placeholder substitution and positional args,
// or accepts pre-built form values (for prepared statements).
func (c *n1qlConn) buildRequestWithArgs(ctx context.Context, query string, args []driver.Value, prebuilt *url.Values) (*http.Request, error) {
	c.mu.RLock()
	api := c.queryAPI
	c.mu.RUnlock()

	var form url.Values

	if prebuilt != nil {
		form = *prebuilt
	} else {
		form = url.Values{}

		if len(args) > 0 {
			replaced, argCount := replacePlaceholders(query)
			if argCount != len(args) {
				return nil, fmt.Errorf("n1ql: argument count mismatch: query has %d placeholders, got %d args", argCount, len(args))
			}
			// Use server-side positional args — never do string substitution.
			form.Set("statement", replaced)
			argJSON, err := buildPositionalArgList(args)
			if err != nil {
				return nil, err
			}
			form.Set("args", argJSON)
		} else {
			form.Set("statement", query)
		}

		c.applyQueryParams(&form)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("n1ql: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req, nil
}

// applyQueryParams appends per-connection query parameters to a form.
func (c *n1qlConn) applyQueryParams(v *url.Values) {
	for k, val := range c.config.QueryParams {
		v.Set(k, val)
	}
}

// -----------------------------------------------------------------------
// Result
// -----------------------------------------------------------------------

type n1qlResult struct {
	affectedRows int64
}

func (r *n1qlResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("n1ql: LastInsertId is not supported")
}

func (r *n1qlResult) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}

// -----------------------------------------------------------------------
// Statement
// -----------------------------------------------------------------------

// n1qlStmt implements driver.Stmt and driver.StmtQueryContext.
type n1qlStmt struct {
	conn     *n1qlConn
	prepared string
	name     string
	argCount int
	mu       sync.Mutex
	closed   bool
}

func (s *n1qlStmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.prepared = ""
	s.name = ""
	return nil
}

func (s *n1qlStmt) NumInput() int { return s.argCount }

func (s *n1qlStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), toNamedValues(args))
}

func (s *n1qlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), toNamedValues(args))
}

// ExecContext implements driver.StmtExecContext.
func (s *n1qlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("n1ql: statement is closed")
	}
	s.mu.Unlock()

	form, err := s.buildForm(args)
	if err != nil {
		return nil, err
	}
	req, err := s.conn.buildRequestWithArgs(ctx, "", nil, form)
	if err != nil {
		return nil, err
	}
	return s.conn.doExec(req)
}

// QueryContext implements driver.StmtQueryContext.
func (s *n1qlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("n1ql: statement is closed")
	}
	name := s.name
	s.mu.Unlock()

	form, err := s.buildForm(args)
	if err != nil {
		return nil, err
	}
	req, err := s.conn.buildRequestWithArgs(ctx, "", nil, form)
	if err != nil {
		return nil, err
	}

	rows, err := s.conn.doQuery(req)
	if err != nil && name != "" {
		// Named prepared statement may have been evicted from the server cache.
		// Retry once using the full prepared JSON instead.
		s.mu.Lock()
		s.name = ""
		s.mu.Unlock()

		form2, _ := s.buildForm(args)
		req2, rerr := s.conn.buildRequestWithArgs(ctx, "", nil, form2)
		if rerr != nil {
			return nil, rerr
		}
		return s.conn.doQuery(req2)
	}
	return rows, err
}

// buildForm constructs the POST form for a prepared statement execution.
func (s *n1qlStmt) buildForm(args []driver.NamedValue) (*url.Values, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}

	if len(vals) < s.argCount {
		return nil, fmt.Errorf("n1ql: insufficient args: need %d, got %d", s.argCount, len(vals))
	}

	form := url.Values{}

	s.mu.Lock()
	name := s.name
	prepared := s.prepared
	s.mu.Unlock()

	if name != "" {
		form.Set("prepared", fmt.Sprintf("%q", name))
	} else {
		form.Set("prepared", prepared)
	}

	if len(vals) > 0 {
		argJSON, err := buildPositionalArgList(vals)
		if err != nil {
			return nil, err
		}
		form.Set("args", argJSON)
	}

	s.conn.applyQueryParams(&form)
	return &form, nil
}

// -----------------------------------------------------------------------
// Error type
// -----------------------------------------------------------------------

type n1qlError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func (e n1qlError) Error() string {
	return fmt.Sprintf("code=%d msg=%s", e.Code, e.Msg)
}

func serializeErrors(errs []n1qlError) string {
	if len(errs) == 0 {
		return "unknown error"
	}
	b := &bytes.Buffer{}
	for i, e := range errs {
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(b, "code=%d msg=%s", e.Code, e.Msg)
	}
	return b.String()
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

// replacePlaceholders converts bare ? markers to $1, $2, … and returns
// the rewritten query and the number of placeholders found.
func replacePlaceholders(query string) (string, int) {
	count := 0
	replaced := placeholderRe.ReplaceAllStringFunc(query, func(string) string {
		count++
		return fmt.Sprintf("$%d", count)
	})
	return replaced, count
}

// buildPositionalArgList serialises driver.Value args to a JSON array string
// suitable for the N1QL "args" parameter.
// Using JSON marshalling ensures correct escaping — no string substitution.
func buildPositionalArgList(args []driver.Value) (string, error) {
	out := make([]interface{}, len(args))
	for i, arg := range args {
		switch v := arg.(type) {
		case []byte:
			// Treat raw bytes as a pre-encoded JSON value.
			out[i] = json.RawMessage(v)
		default:
			out[i] = v
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("n1ql: marshalling args: %w", err)
	}
	return string(b), nil
}

// namedValuesToValues strips the Name/Ordinal wrapper from NamedValues.
func namedValuesToValues(named []driver.NamedValue) ([]driver.Value, error) {
	out := make([]driver.Value, len(named))
	for i, nv := range named {
		if nv.Name != "" {
			return nil, fmt.Errorf("n1ql: named parameters are not supported (got %q)", nv.Name)
		}
		out[i] = nv.Value
	}
	return out, nil
}

// toNamedValues wraps plain Values for use with the Context variants.
func toNamedValues(vals []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(vals))
	for i, v := range vals {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}

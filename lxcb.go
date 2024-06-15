package lxcb

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
)

var params map[string]string
var N1QL_PASSTHROUGH_MODE = false

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 10
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

func SetQueryParams(key string, value string) error {

	if key == "" {
		return fmt.Errorf("N1QL: Key not specified")
	}
	params[key] = value
	return nil
}

// implements Driver interface
type n1qlDriver struct{}

func init() {
	sql.Register("n1ql", &n1qlDriver{})
	params = make(map[string]string)
	//fmt.Println("params...")
}

//username:password@protocol(address)/dbname?param=value
func (n *n1qlDriver) Open(dataSourceName string) (driver.Conn, error) {
	//fmt.Println("open...", dataSourceName)
	return openConn2(dataSourceName)
}

func setQueryParams(v *url.Values) {

	//fmt.Println("paramsLen:", params, len(params))
	for key, value := range params {
		v.Set(key, value)
		//fmt.Println(key, "==>", value)
	}
}

// prepare a http request for the query
func prepareRequest(query string, queryAPI string, args []driver.Value) (*http.Request, error) {

	postData := url.Values{}
	postData.Set("statement", query)

	if len(args) > 0 {
		paStr := buildPositionalArgList(args)
		if len(paStr) > 0 {
			postData.Set("args", paStr)
		}
	}

	setQueryParams(&postData)
	//fmt.Println("prepareRequest...", postData)
	request, _ := http.NewRequest("POST", queryAPI, bytes.NewBufferString(postData.Encode()))
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	return request, nil
}

func openConn2(dataSourceName string) (driver.Conn, error) {

	queryAPI := fmt.Sprintf("%s/query/service", dataSourceName)
	conn := &n1qlConn{client: HTTPClient, queryAPI: queryAPI}

	request, err := prepareRequest("SELECT 1", queryAPI, nil)
	if err != nil {
		return nil, err
	}

	resp, err := conn.client.Do(request)
	//fmt.Println(params, dataSourceName, queryAPI, resp.Status)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		bod, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("N1QL: Connection failure %s", bod)
	}
	return conn, err
}

// implements driver.Conn interface
type n1qlConn struct {
	//clusterAddress string
	queryAPI string
	client   *http.Client
	lock     sync.RWMutex
}

func (conn *n1qlConn) doClientRequest(query string, requestValues *url.Values) (*http.Response, error) {

	ok := false
	for !ok {

		var request *http.Request
		var err error

		// select query API
		//rand.Seed(time.Now().Unix())
		numNodes := 1 //len(conn.queryAPIs)

		//selectedNode := rand.Intn(numNodes)
		conn.lock.RLock()
		queryAPI := conn.queryAPI //conn.queryAPIs[selectedNode]
		conn.lock.RUnlock()

		if query != "" {
			request, err = prepareRequest(query, queryAPI, nil)
			if err != nil {
				return nil, err
			}
		} else {
			if requestValues != nil {
				request, _ = http.NewRequest("POST", queryAPI, bytes.NewBufferString(requestValues.Encode()))
			} else {
				request, _ = http.NewRequest("POST", queryAPI, nil)
			}
			request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		}

		resp, err := conn.client.Do(request)
		if err != nil {
			// if this is the last node return with error
			if numNodes == 1 {
				break
			}
			// remove the node that failed from the list of query nodes
			//conn.lock.Lock()
			//conn.queryAPIs = append(conn.queryAPIs[:selectedNode], conn.queryAPIs[selectedNode+1:]...)
			//conn.lock.Unlock()
			continue
		} else {
			return resp, nil
		}
	}
	return nil, fmt.Errorf("N1QL: Query nodes not responding")
}

func decodeSignature(signature *json.RawMessage) interface{} {

	var sign interface{}
	var rows map[string]interface{}
	json.Unmarshal(*signature, &sign)

	switch s := sign.(type) {
	case map[string]interface{}:
		return s
	case string:
		return s
	default:
		fmt.Printf(" Cannot decode signature. Type of this signature is %T", s)
		return map[string]interface{}{"*": "*"}
	}
	return rows
}

func (conn *n1qlConn) performQuery(query string, requestValues *url.Values) (driver.Rows, error) {

	resp, err := conn.doClientRequest(query, requestValues)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		bod, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s", bod)
	}

	var resultMap map[string]*json.RawMessage
	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(&resultMap)
	if err != nil {
		return nil, fmt.Errorf(" N1QL: Failed to decode result %v", err)
	}

	// fmt.Println(resultMap, len(resultMap), N1QL_PASSTHROUGH_MODE)
	// fmt.Printf("metrics: %c\n", resultMap["metrics"])
	// fmt.Printf("requestID: %c\n", resultMap["requestID"])
	//fmt.Printf("results: %c\n", resultMap["results"])
	// fmt.Printf("signature: %c\n", resultMap["signature"])
	// fmt.Printf("status: %c\n", resultMap["status"])

	var signature interface{}
	var resultRows *json.RawMessage
	var metrics interface{}
	var status interface{}
	var requestId interface{}
	var errs interface{}

	for name, results := range resultMap {
		switch name {
		case "errors":
			_ = json.Unmarshal(*results, &errs)
		case "signature":
			if results != nil {
				signature = decodeSignature(results)
			} else if N1QL_PASSTHROUGH_MODE {
				// for certain types of DML queries, the returned signature could be null
				// however in passthrough mode we always return the metrics, status etc as
				// rows therefore we need to ensure that there is a default signature.
				signature = map[string]interface{}{"*": "*"}
			}
		case "results":
			resultRows = results
		case "metrics":
			if N1QL_PASSTHROUGH_MODE {
				_ = json.Unmarshal(*results, &metrics)
			}
		case "status":
			if N1QL_PASSTHROUGH_MODE {
				_ = json.Unmarshal(*results, &status)
			}
		case "requestID":
			if N1QL_PASSTHROUGH_MODE {
				_ = json.Unmarshal(*results, &requestId)
			}
		}
	}

	if N1QL_PASSTHROUGH_MODE {
		extraVals := map[string]interface{}{"requestID": requestId,
			"status":    status,
			"signature": signature,
		}

		// in passthrough mode last line will always be en error line
		errors := map[string]interface{}{"errors": errs}
		return resultToRows(bytes.NewReader(*resultRows), resp, signature, metrics, errors, extraVals)
	}

	// we return the errors with the rows because we can have scenarios where there are valid
	// results returned along with the error and this interface doesn't allow for both to be
	// returned and hence this workaround.
	return resultToRows(bytes.NewReader(*resultRows), resp, signature, nil, errs, nil)
}

func prepareQuery(query string) (string, int) {

	var count int
	re := regexp.MustCompile("\\?")

	f := func(s string) string {
		count++
		return fmt.Sprintf("$%d", count)
	}
	return re.ReplaceAllStringFunc(query, f), count
}

func serializeErrors(errors interface{}) string {

	var errString string
	switch errors := errors.(type) {
	case []interface{}:
		for _, e := range errors {
			switch e := e.(type) {
			case map[string]interface{}:
				code, _ := e["code"]
				msg, _ := e["msg"]

				if code != 0 && msg != "" {
					if errString != "" {
						errString = fmt.Sprintf("%v Code : %v Message : %v", errString, code, msg)
					} else {
						errString = fmt.Sprintf("Code : %v Message : %v", code, msg)
					}
				}
			}
		}
	}
	if errString != "" {
		return errString
	}
	return fmt.Sprintf(" Error %v %T", errors, errors)
}

func (conn *n1qlConn) Prepare(query string) (driver.Stmt, error) {

	//n1qlStmt
	var argCount int

	query = "PREPARE " + query
	query, argCount = prepareQuery(query)

	resp, err := conn.doClientRequest(query, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		bod, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s", bod)
	}

	var resultMap map[string]*json.RawMessage
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
	}

	if err := json.Unmarshal(body, &resultMap); err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
	}

	stmt := &n1qlStmt{conn: conn, argCount: argCount}

	errors, ok := resultMap["errors"]
	if ok && errors != nil {
		var errs []interface{}
		_ = json.Unmarshal(*errors, &errs)
		return nil, fmt.Errorf("N1QL: Error preparing statement %v", serializeErrors(errs))
	}

	for name, results := range resultMap {

		switch name {

		case "results":
			var preparedResults []interface{}
			if err := json.Unmarshal(*results, &preparedResults); err != nil {
				return nil, fmt.Errorf("N1QL: Failed to unmarshal results %v", err)
			}
			if len(preparedResults) == 0 {
				return nil, fmt.Errorf("N1QL: Unknown error, no prepared results returned")
			}
			serialized, _ := json.Marshal(preparedResults[0])
			stmt.name = preparedResults[0].(map[string]interface{})["name"].(string)
			stmt.prepared = string(serialized)

		case "signature":
			stmt.signature = string(*results)
		}

	}

	if stmt.prepared == "" {
		return nil, fmt.Errorf("internal error")
	}
	return stmt, nil
}

func (conn *n1qlConn) Close() error {

	return nil
}

func (conn *n1qlConn) Begin() (driver.Tx, error) {

	return nil, fmt.Errorf("not supported")
}

func (conn *n1qlConn) Query(query string, args []driver.Value) (driver.Rows, error) {

	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("argument count mismatch %d != %d", argCount, len(args))
		}
		query, _ = preparePositionalArgs(query, argCount, args)
	}
	return conn.performQuery(query, nil)
}

type n1qlResult struct {
	affectedRows int64
	insertId     int64
}

func (res *n1qlResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *n1qlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

func (conn *n1qlConn) performExec(query string, requestValues *url.Values) (driver.Result, error) {

	resp, err := conn.doClientRequest(query, requestValues)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		bod, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s", bod)
	}

	var resultMap map[string]*json.RawMessage
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
	}

	if err := json.Unmarshal(body, &resultMap); err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
	}

	//fmt.Printf("metrics:%c %T\n", resultMap["metrics"], resultMap["metrics"])
	var execErr error
	res := &n1qlResult{}
	for name, results := range resultMap {
		switch name {
		case "metrics":
			var metrics map[string]interface{}
			err := json.Unmarshal(*results, &metrics)
			if err != nil {
				return nil, fmt.Errorf("N1QL: Failed to unmarshal response. Error %v", err)
			}
			if mc, ok := metrics["mutationCount"]; ok {
				res.affectedRows = int64(mc.(float64))
			}
			break

		case "errors":
			var errs []interface{}
			_ = json.Unmarshal(*results, &errs)
			execErr = fmt.Errorf("N1QL: Error executing query %v", serializeErrors(errs))
		}
	}

	return res, execErr
}

// Replace the conditional pqrams in the query and return the list of left-over args
func preparePositionalArgs(query string, argCount int, args []driver.Value) (string, []driver.Value) {

	subList := make([]string, 0)
	newArgs := make([]driver.Value, 0)

	for i, arg := range args {
		if i < argCount {
			var a string
			switch arg := arg.(type) {
			case string:
				a = fmt.Sprintf("\"%v\"", arg)
				//a = fmt.Sprintf("%q", arg)
			case []byte:
				a = string(arg)
			default:
				a = fmt.Sprintf("%v", arg)
			}
			sub := []string{fmt.Sprintf("$%d", i+1), a}
			subList = append(subList, sub...)
		} else {
			newArgs = append(newArgs, arg)
		}
	}
	r := strings.NewReplacer(subList...)
	return r.Replace(query), newArgs
}

func (conn *n1qlConn) Exec(query string, args []driver.Value) (driver.Result, error) {

	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("argument count mismatch %d != %d", argCount, len(args))
		}
		query, _ = preparePositionalArgs(query, argCount, args)
	}
	return conn.performExec(query, nil)
}

// implements driver.Stmt interface
type n1qlStmt struct {
	conn      *n1qlConn
	prepared  string
	signature string
	argCount  int
	name      string
}

func (stmt *n1qlStmt) Close() error {

	stmt.prepared = ""
	stmt.signature = ""
	stmt.argCount = 0
	stmt = nil
	return nil
}

func (stmt *n1qlStmt) NumInput() int {
	return stmt.argCount
}

func buildPositionalArgList(args []driver.Value) string {
	positionalArgs := make([]string, 0)
	for _, arg := range args {
		switch arg := arg.(type) {
		case string:
			// add double quotes since this is a string
			positionalArgs = append(positionalArgs, fmt.Sprintf("\"%v\"", arg))
		case []byte:
			positionalArgs = append(positionalArgs, string(arg))
		default:
			positionalArgs = append(positionalArgs, fmt.Sprintf("%v", arg))
		}
	}

	if len(positionalArgs) > 0 {
		paStr := "["
		for i, param := range positionalArgs {
			if i == len(positionalArgs)-1 {
				paStr = fmt.Sprintf("%s%s]", paStr, param)
			} else {
				paStr = fmt.Sprintf("%s%s,", paStr, param)
			}
		}
		return paStr
	}
	return ""
}

// prepare a http request for the query
func (stmt *n1qlStmt) prepareRequest(args []driver.Value) (*url.Values, error) {

	postData := url.Values{}

	// use name prepared statement if possible
	if stmt.name != "" {
		postData.Set("prepared", fmt.Sprintf("\"%s\"", stmt.name))
	} else {
		postData.Set("prepared", stmt.prepared)
	}

	if len(args) < stmt.NumInput() {
		return nil, fmt.Errorf("N1QL: Insufficient args. Prepared statement contains positional args")
	}

	if len(args) > 0 {
		paStr := buildPositionalArgList(args)
		if len(paStr) > 0 {
			postData.Set("args", paStr)
		}
	}
	setQueryParams(&postData)
	return &postData, nil
}

func (stmt *n1qlStmt) Exec(args []driver.Value) (driver.Result, error) {

	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}
	return stmt.conn.performExec("", requestValues)
}

func (stmt *n1qlStmt) Query(args []driver.Value) (driver.Rows, error) {

	fmt.Println("stmtQuery", args, len(args))
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}

retry:
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.conn.performQuery("", requestValues)
	if err != nil && stmt.name != "" {
		// retry once if we used a named prepared statement
		stmt.name = ""
		goto retry
	}
	return rows, err
}

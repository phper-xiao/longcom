package request

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/trpc-group/trpc-go/log"
)

const (
	MethodPost = "POST"
	MethodGet  = "GET"

	// DefaultConnTimeout Conn takes a timeout.
	DefaultConnTimeout = 3
)

// HTTPProxy http 实体
type HTTPProxy struct {
	URL     string
	Method  string
	ReqBody []byte
	Headers map[string]string
	Timeout time.Duration
}

func timeout(sec int64) time.Duration {
	if sec <= 0 {
		sec = DefaultConnTimeout
	}
	return time.Duration(sec) * time.Second
}

// NewHTTP return http proxy ins
func NewHTTP(url, method string, reqBody []byte, headers map[string]string, timeoutSec int64) *HTTPProxy {
	return &HTTPProxy{
		URL:     url,
		Method:  method,
		ReqBody: reqBody,
		Headers: headers,
		Timeout: timeout(timeoutSec),
	}
}

// Request http请求
func (p *HTTPProxy) Request() (rsp []byte, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)

	if !strings.Contains(p.URL, "http://") && !strings.Contains(p.URL, "https://") {
		err = errors.New("not http(s) request")
		return
	}

	client := http.Client{
		Timeout: p.Timeout, //超时
	}
	if req, err = http.NewRequest(p.Method, p.URL, bytes.NewReader(p.ReqBody)); err != nil {
		return
	}

	for key, value := range p.Headers {
		req.Header.Set(key, value)
	}

	if resp, err = client.Do(req); err != nil {
		return nil, fmt.Errorf("http request failed. err: %s", err.Error())
	}

	if resp == nil {
		return
	}

	defer resp.Body.Close()

	if rsp, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}

	log.InfoContextf(context.Background(),
		"HttpPost|http post request|url=%s|params=%s|response=%s",
		p.URL,
		string(p.ReqBody),
		string(rsp),
	)

	return
}

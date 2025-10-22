package request

import (
	"context"
	"io/ioutil"
	"strings"
	"time"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
	"github.com/trpc-group/trpc-go/log"
)

const (
	HttpTimeout = 2000 // 2000 ms
)

// Hc 客户端连接
var HttpObj *WrapHttpClient

// WrapHttpClient 客户端连接
type WrapHttpClient struct {
	Client *retryablehttp.Client
}

// init 初始化客户端连接Hc
func init() {
	CreateHttpClient()
}

// CreateHttpClient 默认初始化参数：一个http请求，最大超时时长600+200*3=1.2s
func CreateHttpClient(opts ...Option) {
	Hc := retryablehttp.NewClient()
	// 第一次重试等待时长： 150*2^0=150ms
	// 第二次重试等待时长： 150*2^1=300ms
	// 第三次重试等待时长:  150*2^2=600ms
	Hc.RetryWaitMin = 150 * time.Millisecond // 150ms
	Hc.RetryWaitMax = 600 * time.Millisecond // 600ms
	Hc.RetryMax = 0                          // 最大重试3次
	Hc.HTTPClient.Timeout = time.Duration(HttpTimeout) * time.Millisecond
	for _, o := range opts {
		o(Hc)
	}
	HttpObj = &WrapHttpClient{
		Client: Hc,
	}
}

// *******retryablehttp.Client参数设置**********
type Option func(*retryablehttp.Client)

// WithRetryWaitDuration http请求失败重试[min, max]时间
// get value between (min*2^retryCount) and (max)
func WithRetryWaitDuration(min time.Duration, max time.Duration) Option {
	return func(c *retryablehttp.Client) {
		c.RetryWaitMin = min
		c.RetryWaitMax = max
	}
}

// WithRetryMaxCount 最大重试次数, 默认值：3次
func WithRetryMaxCount(max int) Option {
	return func(c *retryablehttp.Client) {
		c.RetryMax = max
	}
}

// WithTimeout 请求超时时长， 默认：150ms
func WithTimeout(timeout time.Duration) Option {
	return func(c *retryablehttp.Client) {
		c.HTTPClient.Timeout = timeout
	}
}

// HttpGet http get
func (h *WrapHttpClient) HttpGet(ctx context.Context, url string, headers interface{}) (interface{}, error) {
	req, err := retryablehttp.NewRequest("GET", url, nil)
	if err != nil {
		log.WarnContextf(ctx, "HttpGet|new http request failed|err=%v", err)
		return nil, err
	}
	// set headers
	if headers != nil {
		for key, val := range headers.(map[string]string) {
			req.Header.Set(key, val)
		}
	}
	resp, err := h.Client.Do(req)
	if err != nil && resp == nil {
		log.WarnContextf(ctx, "HttpGet|do get request failed|err=%v", err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WarnContextf(ctx, "HttpGet|read response body failed|err=%v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "HttpGet|url=%s", url)
	return body, nil
}

// HttpPost http post
func (h *WrapHttpClient) HttpPost(ctx context.Context, url string, postParams string, headers interface{}) ([]byte, error) {
	req, err := retryablehttp.NewRequest("POST", url, strings.NewReader(postParams))
	if err != nil {
		log.WarnContextf(ctx, "HttpPost|new http request failed|err=%v", err)
		return nil, err
	}
	// default content-type json
	req.Header.Set("Content-Type", "application/json")
	// set headers
	if headers != nil {
		for key, val := range headers.(map[string]string) {
			req.Header.Set(key, val)
		}
	}
	resp, err := h.Client.Do(req)
	if err != nil && resp == nil {
		log.WarnContextf(ctx, "HttpPost|do post request failed|err=%v", err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WarnContextf(ctx, "HttpPost|read response body failed|err=%v", err)
		return nil, err
	}
	log.InfoContextf(ctx,
		"HttpPost|http post request|url=%s|params=%s|response=%s",
		url,
		postParams,
		string(body),
	)
	return body, nil
}

package main

import (
    "context"
    "fmt"
    "math/rand"
    "net/http"
    "os"
    "path/filepath"
    "sync"
    "time"

    _ "github.com/tylerxiao/longcom/repo/accesslog"
    _ "go.uber.org/automaxprocs"

    "github.com/judwhite/go-svc/svc"
    "github.com/tylerxiao/longcom/broker"
    "github.com/tylerxiao/longcom/config"
    "github.com/tylerxiao/longcom/repo/ws"
    trpc "trpc.group/trpc-go/trpc-go"
    "trpc.group/trpc-go/trpc-go/filter"
    "trpc.group/trpc-go/trpc-go/log"
    "trpc.group/trpc-go/trpc-go/server"
)

type program struct {
    once   sync.Once
    server *broker.Server
}

// Init
func (p *program) Init(env svc.Environment) error {
    if env.IsWindowsService() {
        dir := filepath.Dir(os.Args[0])
        return os.Chdir(dir)
    }
    return nil
}

// Start instantiate broker and start
func (p *program) Start() error {
    rand.Seed(time.Now().UTC().UnixNano())

    // 注入 cancer context （用于联动关闭websocket）
    var cctx, cancel = context.WithCancel(context.Background())
    wsCloseFilter := filter.ServerFilter(func(ctx context.Context, req interface{},
        next filter.ServerHandleFunc) (rsp interface{}, err error) {
        ctx = context.WithValue(ctx, ws.WSCancel, cctx)
        return next(ctx, req)
    })

    s := trpc.NewServer(server.WithFilter(wsCloseFilter))
    s.RegisterOnShutdown(cancel) // 注入关闭函数
    server, err := broker.NewServer(s, config.ServerConfig)
    if err != nil {
        log.Fatalf("failed to instantiate broker - %+v", err)
        fmt.Printf("failed to instantiate broker - %+v", err)
    }
    p.server = server

    go func() {
        err := p.server.Start()
        if err != nil {
            log.Errorf("failed to start broker - %+v", err)
            fmt.Printf("failed to start broker - %+v", err)
            _ = p.Stop()
            os.Exit(1)
        }
    }()

    return nil
}

// Stop exit broker
func (p *program) Stop() error {
    p.once.Do(func() {
        p.server.Exit()
    })
    return nil
}

func main() {
    go func() {
        log.Info(http.ListenAndServe("0.0.0.0:6060", nil))
    }()

    prg := &program{}
    if err := svc.Run(prg); err != nil {
        log.Fatalf("%s", err)
    }
}

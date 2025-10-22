module github.com/tylerxiao/longcom

go 1.17

require (
    github.com/BurntSushi/toml v1.1.0
    github.com/gomodule/redigo v2.0.0+incompatible
    github.com/google/uuid v1.3.0
    github.com/trpc-go/trpc-go v1.0.3
    go.uber.org/zap v1.21.0
)

require (
    github.com/Shopify/sarama v1.29.1
    github.com/cenkalti/backoff/v4 v4.1.3
    github.com/dgrijalva/jwt-go/v4 v4.0.0-preview1
    github.com/gobwas/httphead v0.1.0
    github.com/gobwas/pool v0.2.1
    github.com/gobwas/ws v1.1.0
    github.com/golang/protobuf v1.5.2
    github.com/gorilla/websocket v1.5.0
    github.com/hashicorp/go-retryablehttp v0.7.0
    github.com/judwhite/go-svc v1.1.3
    github.com/panjf2000/ants/v2 v2.4.7
    github.com/panjf2000/gnet v1.6.6
    github.com/pkg/errors v0.9.1
    github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7
    github.com/xdg/scram v1.0.3
    go.uber.org/atomic v1.9.0
    go.uber.org/automaxprocs v1.4.0
    google.golang.org/protobuf v1.28.0
)

replace git.code.oa.com/up-common/rpcprotocol/svip_longcom => ./protocols/svip_longcom

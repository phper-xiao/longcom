package broker

import (
	"context"
	"time"
)

// ConnectionSession session of connection
type ConnectionSession struct {
	ConnectionID string `json:"connectionID"`
	Business     string `json:"business"`
	AuthData     []byte `json:"authData"`
	Alias        string `json:"alias"`
	Callback     string `json:"callback"`
	Topic        string `json:"topic"`
	ConnectTime  int64  `json:"connectTime"`
}

func newConnectionSession(
	ctx context.Context,
	connectionID string,
	business string,
	authData []byte,
	alias string,
	callback string,
	topic string,
) (*ConnectionSession, error) {
	return &ConnectionSession{
		ConnectionID: connectionID,
		Business:     business,
		AuthData:     authData,
		Alias:        alias,
		Callback:     callback,
		Topic:        topic,
		ConnectTime:  time.Now().Unix(),
	}, nil
}

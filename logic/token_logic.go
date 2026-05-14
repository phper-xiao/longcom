package logic

import (
	"context"
	"errors"
	"time"

    "github.com/tylerxiao/longcom/config"
	"github.com/dgrijalva/jwt-go/v4"
	"trpc.group/trpc-go/trpc-go/log"
	"go.uber.org/zap"
)

// LoginClaims custom claims
type LoginClaims struct {
	AppName     string `json:"app_name"`
	Topic       string `json:"topic"`
	UserID      string `json:"user_id"`
	MessageType int32  `json:"message_type"`
	CallbackURL string `json:"callback_url"`
	jwt.StandardClaims
}

// GetToken 获取websocket token
func GetToken(ctx context.Context, appName, topic, userID, callbackURL string, messageType int32) (string, error) {
	claims := LoginClaims{
		AppName:     appName,
		Topic:       topic,
		UserID:      userID,
		CallbackURL: callbackURL,
		MessageType: messageType,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: jwt.NewTime(float64(time.Now().Add(30 * time.Second).Unix())),
		},
	}
	tokenClaims := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := tokenClaims.SignedString(config.JWTSecret)
	if err != nil {
		log.WarnContextf(ctx, "Get jwt Token fail", zap.String("app_name", appName),
			zap.String("user_id", userID), zap.Error(err))
		return "", err
	}
	return token, nil
}

// VerifyToken 解析websocket token
func VerifyToken(ctx context.Context, token string) (claims *LoginClaims, err error) {
	tokenClaims, err := jwt.ParseWithClaims(token, &LoginClaims{}, func(token *jwt.Token) (interface{}, error) {
		return config.JWTSecret, nil
	})
	if err != nil {
		log.WarnContextf(ctx, "Parse jwt Token fail", zap.String("token", token), zap.Error(err))
		return
	}

	if !tokenClaims.Valid {
		log.WarnContextf(ctx, "Parse jwt Token invalid", zap.String("token", token), zap.Error(err))
		return nil, errors.New("unauthorized")
	}

	if claims, ok := tokenClaims.Claims.(*LoginClaims); ok {
		return claims, nil
	}

	log.WarnContextf(ctx, "Parse jwt claims invalid", zap.String("token", token), zap.Error(err))
	return nil, errors.New("unauthorized")
}

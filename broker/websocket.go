/**********************************************************************************
* Copyright (c) 2009-2019 Misakai Ltd.
* This program is free software: you can redistribute it and/or modify it under the
* terms of the GNU Affero General Public License as published by the  Free Software
* Foundation, either version 3 of the License, or(at your option) any later version.
*
* This program is distributed  in the hope that it  will be useful, but WITHOUT ANY
* WARRANTY;  without even  the implied warranty of MERCHANTABILITY or FITNESS FOR A
* PARTICULAR PURPOSE.  See the GNU Affero General Public License  for  more details.
*
* You should have  received a copy  of the  GNU Affero General Public License along
* with this program. If not, see<http://www.gnu.org/licenses/>.
************************************************************************************/

package broker

import (
	"bytes"
	"net"
	"sync"
	"time"

	"io"

    "github.com/tylerxiao/longcom/repo/merrors"
    "github.com/tylerxiao/longcom/repo/ws"
    "github.com/tylerxiao/longcom/repo/ws/wsutil"
	"github.com/gorilla/websocket"
	"github.com/panjf2000/gnet"
	"github.com/trpc-group/trpc-go/log"
)

// websocketConn represents a connection of websocket.
type websocketConn interface {
	// returns a writer for the next message to send. The writer's Close
	// method flushes the complete message to the network.
	//
	// There can be at most one open writer on a connection. NextWriter closes the
	// previous writer if the application has not already done so.
	//
	// All message types (TextMessage, BinaryMessage, CloseMessage, PingMessage and
	// PongMessage) are supported.
	NextReader() (messageType int, r io.Reader, err error)
	// returns a writer for the next message to send. The writer's Close
	// method flushes the complete message to the network.
	//
	// There can be at most one open writer on a connection. NextWriter closes the
	// previous writer if the application has not already done so.
	//
	// All message types (TextMessage, BinaryMessage, CloseMessage, PingMessage and
	// PongMessage) are supported.
	NextWriter(messageType int) (io.WriteCloser, error)
	// terminates the connection.
	Close() error
	// returns the local network address.
	LocalAddr() net.Addr
	//returns the remote network address.
	RemoteAddr() net.Addr
	// sets the deadline for future Read calls
	// and any currently-blocked Read call.
	SetReadDeadline(t time.Time) error
	// sets the deadline for future Write calls
	// and any currently-blocked Write call.
	SetWriteDeadline(t time.Time) error
}

// websocketConn represents a websocket connection.
type websocketTransport struct {
	sync.Mutex
	socket  websocketConn
	reader  io.Reader
	closing chan bool
}

const (
	writeWait        = 10 * time.Second    // Time allowed to write a message to the peer.
	pongWait         = 60 * time.Second    // Time allowed to read the next pong message from the peer.
	pingPeriod       = (pongWait * 9) / 10 // Send pings to peer with this period. Must be less than pongWait.
	closeGracePeriod = 10 * time.Second    // Time to wait before force close on connection.
)

// newConn creates a new transport from websocket.
func NewWebsocketConn(ws websocketConn) net.Conn {
	conn := &websocketTransport{
		socket:  ws,
		closing: make(chan bool),
	}

	return conn
}

// Read reads data from the connection. It is possible to allow reader to time
// out and return a Error with Timeout() == true after a fixed time limit by
// using SetDeadline and SetReadDeadline on the websocket.
func (c *websocketTransport) Read(b []byte) (n int, err error) {
	var opCode int
	if c.reader == nil {
		// New message
		var r io.Reader
		for {
			if opCode, r, err = c.socket.NextReader(); err != nil {
				return
			}

			if opCode != websocket.BinaryMessage && opCode != websocket.TextMessage {
				continue
			}

			c.reader = r
			break
		}
	}

	// Read from the reader
	n, err = c.reader.Read(b)
	if err != nil {
		if err == io.EOF {
			c.reader = nil
			err = nil
		}
	}
	return
}

// Write writes data to the connection. It is possible to allow writer to time
// out and return a Error with Timeout() == true after a fixed time limit by
// using SetDeadline and SetWriteDeadline on the websocket.
func (c *websocketTransport) Write(b []byte) (n int, err error) {
	// Serialize write to avoid concurrent write
	c.Lock()
	defer c.Unlock()

	var w io.WriteCloser
	if w, err = c.socket.NextWriter(websocket.BinaryMessage); err == nil {
		if n, err = w.Write(b); err == nil {
			err = w.Close()
		}
	}
	return
}

// Close terminates the connection.
func (c *websocketTransport) Close() error {
	return c.socket.Close()
}

// LocalAddr returns the local network address.
func (c *websocketTransport) LocalAddr() net.Addr {
	return c.socket.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *websocketTransport) RemoteAddr() net.Addr {
	return c.socket.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated
// with the connection. It is equivalent to calling both
// SetReadDeadline and SetWriteDeadline.
func (c *websocketTransport) SetDeadline(t time.Time) (err error) {
	if err = c.socket.SetReadDeadline(t); err == nil {
		err = c.socket.SetWriteDeadline(t)
	}
	return
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
func (c *websocketTransport) SetReadDeadline(t time.Time) error {
	return c.socket.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
func (c *websocketTransport) SetWriteDeadline(t time.Time) error {
	return c.socket.SetWriteDeadline(t)
}

// gnet 使用的 websocket 帧编解码
type WebsocketFrameCodec struct {
}

// Encode websocket frame encode
func (cc *WebsocketFrameCodec) Encode(c gnet.Conn, buf []byte) ([]byte, error) {
	return buf, nil
}

// Decode websocket frame codec
func (cc *WebsocketFrameCodec) Decode(c gnet.Conn) ([]byte, error) {
	ungradedIF := c.Context()

	if ungradedIF == nil {
		buf := c.Read()

		readoffset, header, err := ws.DefaultUpgrader.TakeUpgradeHTTPHeader(buf)

		if err != nil {
			if err != merrors.ErrGnetWebsocketCodecNeedMoreData {
				log.Warnf("WebsocketFrameCodec|Decode|err=%+v|", err)
				c.Close()
			}
			return nil, err
		}
		c.ShiftN(readoffset)
		return header, nil
	} else {
		buf := c.Read()
		if len(buf) == 0 {
			return nil, merrors.ErrGnetWebsocketCodecNeedMoreData
		}

		readoffset, frame, err := ws.ReadFrameFromBytes(buf)

		if err != nil {
			if err != merrors.ErrGnetWebsocketCodecNeedMoreData {
				c.Close()
			}
			return nil, err
		}

		c.ShiftN(readoffset)

		return frame, nil
	}
}

// OnSalmonFrameFunc 接收frame处理handler
type OnSalmonFrameFunc func(frame []byte)

// OnSalmonAuthFunc auth处理handler
type OnSalmonAuthFunc func(token []byte)

// OnPing ping处理handler
type OnSalmonPingFunc func(token []byte)

type salmonFrameConn struct {
	c             gnet.Conn
	remoteAddr    string
	b             []byte
	onSalmonFrame OnSalmonFrameFunc
	onSalmonAuth  OnSalmonAuthFunc
	onSalmonPing  OnSalmonPingFunc
}

func newSalmonFrameConn(
	c gnet.Conn,
	onSalmonFrame OnSalmonFrameFunc,
	onSalmonAuth OnSalmonAuthFunc,
	onSalmonPing OnSalmonPingFunc,
) *salmonFrameConn {
	return &salmonFrameConn{
		c:             c,
		remoteAddr:    c.RemoteAddr().String(),
		b:             make([]byte, 0),
		onSalmonFrame: onSalmonFrame,
		onSalmonAuth:  onSalmonAuth,
		onSalmonPing:  onSalmonPing,
	}
}

func (h *salmonFrameConn) RemoteAddr() net.Addr {
	return h.c.RemoteAddr()
}

// OnReact on react
func (h *salmonFrameConn) OnReact(in []byte) (out []byte, action gnet.Action) {
	if h.c.Context() == nil {
		// 未Upgrade，先进行 upgrade
		return h.onReactBeforeUpgrade(in)
	} else {
		return h.onReactAfterUpgrade(in)
	}
}

func (h *salmonFrameConn) onReactBeforeUpgrade(in []byte) (out []byte, action gnet.Action) {
	var err error

	lines := bytes.Split(in, []byte("\r\n"))
	req, err := ws.HttpParseRequestLine(lines[0])
	if err != nil {
		log.Warnf("onReactBeforeUpgrade|remoteAddr=%s|err=%+v|", h.remoteAddr, err)
		action = gnet.Close
		return
	}

	// connection auth
	h.onSalmonAuth(req.URI)

	out, _, err = ws.DefaultUpgrader.Upgrade(in)

	if err != nil {
		// log.Warnf("onReactBeforeUpgrade|remoteAddr=%s|err=%+v|", h.remoteAddr, err)
		action = gnet.Close
		return
	}

	// 设置为 已经 upgraded
	h.c.SetContext(true)
	return
}

func (h *salmonFrameConn) onReactAfterUpgrade(frame []byte) (out []byte, action gnet.Action) {
	// 传入的 Frame 经过 WeboscketCodec，必然是一个完整的数据帧
	reader := bytes.NewReader(frame)
	// frame 是完整的，必读出 一个 完整的Message
	msgs, err := wsutil.ReadClientMessage(reader, nil)
	if err != nil {
		log.Warnf("onReactAfterUpgrade|remoteAddr=%s|err=%+v|", h.remoteAddr, err)
		action = gnet.Close
		return
	}

	if len(msgs) != 1 {
		log.Warnf("onReactAfterUpgrade|remoteAddr=%s|err=Read Msgs Length Invalid|", h.remoteAddr, err)
		action = gnet.Close
		return
	}

	msg := msgs[0]

	if msg.OpCode.IsControl() {
		outWriter := bytes.NewBuffer(nil)
		err := wsutil.HandleClientControlMessage(outWriter, msg)
		if err != nil {
			log.Warnf("onReactAfterUpgrade|remoteAddr=%s|err=Read Msgs Length Invalid|", h.remoteAddr)
			action = gnet.Close
			return
		}

		if msg.OpCode == ws.OpClose {
			action = gnet.Close
		}

		if msg.OpCode == ws.OpPing {
			// 回 pong 包
			h.onSalmonPing(msg.Payload)
		}

		out = outWriter.Bytes()
		return
	}

	if msg.OpCode == ws.OpBinary || msg.OpCode == ws.OpText {
		// 只处理 Binary 数据帧和Text数据帧
		out, action = h.onBinaryData(msg.Payload)
	}

	return
}

// AsyncWrite async write to peer
func (h *salmonFrameConn) AsyncWrite(b []byte) error {
	return h.c.AsyncWrite(b)
}

func (h *salmonFrameConn) Close() {
	h.c.Close()
}

func (h *salmonFrameConn) onBinaryData(in []byte) (out []byte, action gnet.Action) {
	log.Debugf("onBinaryData|remoteAddr=%s|inData=%+v|", h.remoteAddr, string(in))
	h.onSalmonFrame(in)
	return
}

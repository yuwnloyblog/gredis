package gredis
import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type Conn struct {
	conn         net.Conn
	bw           *bufio.Writer
	br           *bufio.Reader
	readTimeout  time.Duration
	writeTimeout time.Duration
	password     string
	db           int

	mu      sync.Mutex
	pending int
	err     error
	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte

	// Scratch space for formatting integers and floats.
	numScratch [40]byte
}

func Dial(network, address string) (*Conn, error) {
	netConn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		conn:     netConn,
		bw:       bufio.NewWriter(netConn),
		br:       bufio.NewReader(netConn),
		password: "",
		db:       0,
	}
	if c.password != "" {

	}
	if c.db != 0 {

	}
	return c, nil
}

func (self *Conn) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()
	err := self.err
	if self.err == nil {
		self.err = errors.New("gredis: closed")
		err = self.conn.Close()
	}
	return err
}

func (self *Conn) fatal(err error) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.err == nil {
		self.err = err
		self.conn.Close()
	}
	return err
}

func (self *Conn) Err() error {
	self.mu.Lock()
	defer self.mu.Unlock()
	err := self.err
	return err
}

func (self *Conn) writeLen(prefix byte, n int) error {
	self.lenScratch[len(self.lenScratch)-1] = '\n'
	self.lenScratch[len(self.lenScratch)-2] = '\r'
	i := len(self.lenScratch) - 3
	for {
		self.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}
	self.lenScratch[i] = prefix
	_, err := self.bw.Write(self.lenScratch[i:])
	return err
}

func (self *Conn) writeString(s string) error {
	self.writeLen('$', len(s))
	self.bw.WriteString(s)
	_, err := self.bw.WriteString("\r\n")
	return err
}

func (self *Conn) writeBytes(p []byte) error {
	self.writeLen('$', len(p))
	self.bw.Write(p)
	_, err := self.bw.WriteString("\r\n")
	return err
}

func (self *Conn) writeInt64(n int64) error {
	return self.writeBytes(strconv.AppendInt(self.numScratch[:0], n, 10))
}

func (self *Conn) writeFloat64(n float64) error {
	return self.writeBytes(strconv.AppendFloat(self.numScratch[:0], n, 'g', -1, 64))
}

func (self *Conn) writeCommand(cmd string, args []interface{}) error {
	self.writeLen('*', 1+len(args))
	err := self.writeString(cmd)
	for _, arg := range args {
		if err != nil {
			break
		}
		switch arg := arg.(type) {
		case string:
			err = self.writeString(arg)
		case []byte:
			err = self.writeBytes(arg)
		case int:
			err = self.writeInt64(int64(arg))
		case int64:
			err = self.writeInt64(arg)
		case float64:
			err = self.writeFloat64(arg)
		case bool:
			if arg {
				err = self.writeString("1")
			} else {
				err = self.writeString("0")
			}
		case nil:
			err = self.writeString("")
		default:
			var buf bytes.Buffer
			fmt.Fprint(&buf, arg)
			err = self.writeBytes(buf.Bytes())
		}
	}
	return err
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("gredis: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

type Error string

func (err Error) Error() string { return string(err) }

func (self *Conn) readLine() ([]byte, error) {
	p, err := self.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("long response line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	return p[:i], nil
}

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.
func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}
func (self *Conn) readReply() (interface{}, error) {
	line, err := self.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == '0' && line[2] == 'K':
			//Avoid allocation for frequent "+OK" response.
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			// Avoid allocation in PING command benchmarks :)
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(string(line[1:])), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(self.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := self.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = self.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, protocolError("unexpected response line")
}

func (self *Conn) Send(cmd string, args ...interface{}) error {
	self.mu.Lock()
	self.pending += 1
	self.mu.Unlock()
	if self.writeTimeout != 0 {
		self.conn.SetWriteDeadline(time.Now().Add(self.writeTimeout))
	}
	if err := self.writeCommand(cmd, args); err != nil {
		return self.fatal(err)
	}
	return nil
}

func (self *Conn) Flush() error {
	if self.writeTimeout != 0 {
		self.conn.SetWriteDeadline(time.Now().Add(self.writeTimeout))
	}
	if err := self.bw.Flush(); err != nil {
		return self.fatal(err)
	}
	return nil
}

func (self *Conn) Receive() (reply interface{}, err error) {
	if self.readTimeout != 0 {
		self.conn.SetReadDeadline(time.Now().Add(self.readTimeout))
	}
	if reply, err = self.readReply(); err != nil {
		return nil, self.fatal(err)
	}
	// When using pub/sub, the number of receives can be greater than the
	// number of sends. To enable normal use of the connection after
	// unsubscribing from all channels, we do not decrement pending to a
	// negative value.
	//
	// The pending field is decremented after the reply is read to handle the
	// case where Receive is called before Send.
	self.mu.Lock()
	if self.pending > 0 {
		self.pending -= 1
	}
	self.mu.Unlock()
	if err, ok := reply.(Error); ok {
		return nil, err
	}
	return
}
func (self *Conn) Do(cmd string, args ...interface{}) (interface{}, error) {
	self.mu.Lock()
	pending := self.pending
	self.pending = 0
	self.mu.Unlock()

	if cmd == "" && pending == 0 {
		return nil, nil
	}
	if self.writeTimeout != 0 {
		self.conn.SetWriteDeadline(time.Now().Add(self.writeTimeout))
	}
	if cmd != "" {
		if err := self.writeCommand(cmd, args); err != nil {
			return nil, self.fatal(err)
		}
	}
	if err := self.bw.Flush(); err != nil {
		return nil, self.fatal(err)
	}
	if self.readTimeout != 0 {
		self.conn.SetReadDeadline(time.Now().Add(self.readTimeout))
	}
	if cmd == "" {
		reply := make([]interface{}, pending)
		for i := range reply {
			r, e := self.readReply()
			if e != nil {
				return nil, self.fatal(e)
			}
			reply[i] = r
		}
		return reply, nil
	}
	var err error
	var reply interface{}
	for i := 0; i <= pending; i++ {
		var e error
		if reply, e = self.readReply(); e != nil {
			return nil, self.fatal(err)
		}
		if e, ok := reply.(Error); ok && err == nil {
			err = e
		}
	}
	return reply, err
}
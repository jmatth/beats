package transport

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type Client struct {
	dialer  Dialer
	network string
	host    string

	conn  net.Conn
	mutex sync.Mutex
}

type Config struct {
	Proxy   *ProxyConfig
	TLS     *TLSConfig
	Timeout time.Duration
	Stats   *IOStats
}

func MakeDialer(c *Config) (Dialer, error) {
	var err error
	dialer := NetDialer(c.Timeout)
	dialer, err = ProxyDialer(c.Proxy, dialer)
	if err != nil {
		return nil, err
	}
	if c.Stats != nil {
		dialer = StatsDialer(dialer, c.Stats)
	}

	if c.TLS != nil {
		return TLSDialer(dialer, c.TLS, c.Timeout)
	}
	return dialer, nil
}

func NewClient(c *Config, network, host string, defaultPort int) (*Client, error) {
	// do some sanity checks regarding network and Config matching +
	// address being parseable
	switch network {
	case "tcp", "tcp4", "tcp6":
	case "udp", "udp4", "udp6":
		if c.TLS == nil && c.Proxy == nil {
			break
		}
		fallthrough
	default:
		return nil, fmt.Errorf("unsupported network type %v", network)
	}

	dialer, err := MakeDialer(c)
	if err != nil {
		return nil, err
	}

	return NewClientWithDialer(dialer, network, host, defaultPort)
}

func NewClientWithDialer(d Dialer, network, host string, defaultPort int) (*Client, error) {
	// check address being parseable
	host = fullAddress(host, defaultPort)
	_, _, err := net.SplitHostPort(host)
	if err != nil {
		return nil, err
	}

	client := &Client{
		dialer:  d,
		network: network,
		host:    host,
	}
	return client, nil
}

func (c *Client) Connect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	conn, err := c.dialer.Dial(c.network, c.host)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) IsConnected() bool {
	c.mutex.Lock()
	b := c.conn != nil
	c.mutex.Unlock()
	return b
}

func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		debugf("closing")
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *Client) getConn() net.Conn {
	c.mutex.Lock()
	conn := c.conn
	c.mutex.Unlock()
	return conn
}

func (c *Client) Read(b []byte) (int, error) {
	conn := c.getConn()
	if conn == nil {
		return 0, ErrNotConnected
	}

	n, err := conn.Read(b)
	return n, c.handleError(err)
}

func (c *Client) Write(b []byte) (int, error) {
	conn := c.getConn()
	if conn == nil {
		return 0, ErrNotConnected
	}

	n, err := c.conn.Write(b)
	return n, c.handleError(err)
}

func (c *Client) LocalAddr() net.Addr {
	conn := c.getConn()
	if conn != nil {
		return c.conn.LocalAddr()
	}
	return nil

}

func (c *Client) RemoteAddr() net.Addr {
	conn := c.getConn()
	if conn != nil {
		return c.conn.LocalAddr()
	}
	return nil
}

func (c *Client) SetDeadline(t time.Time) error {
	conn := c.getConn()
	if conn == nil {
		return ErrNotConnected
	}

	err := conn.SetDeadline(t)
	return c.handleError(err)
}

func (c *Client) SetReadDeadline(t time.Time) error {
	conn := c.getConn()
	if conn == nil {
		return ErrNotConnected
	}

	err := conn.SetReadDeadline(t)
	return c.handleError(err)
}

func (c *Client) SetWriteDeadline(t time.Time) error {
	conn := c.getConn()
	if conn == nil {
		return ErrNotConnected
	}

	err := conn.SetWriteDeadline(t)
	return c.handleError(err)
}

func (c *Client) handleError(err error) error {
	if err != nil {
		debugf("handle error: %v", err)

		if nerr, ok := err.(net.Error); !(ok && (nerr.Temporary() || nerr.Timeout())) {
			_ = c.Close()
		}
	}
	return err
}

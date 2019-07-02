package pqtimeouts

import (
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

const (
	idleKeepAliveStartSeconds     = 30
	keepAliveProbeIntervalSeconds = 5
	keepAliveProbeCnt             = 6
)

type timeoutDialer struct {
	netDial        func(string, string) (net.Conn, error)                // Allow this to be stubbed for testing
	netDialTimeout func(string, string, time.Duration) (net.Conn, error) // Allow this to be stubbed for testing
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

func (t timeoutDialer) Dial(network string, address string) (net.Conn, error) {
	// If we don't have any timeouts set, just return a normal connection
	if t.readTimeout == 0 && t.writeTimeout == 0 {
		return t.netDial(network, address)
	}

	// Otherwise we want a timeoutConn to handle the read and write deadlines for us.
	c, err := t.netDial(network, address)
	if err != nil || c == nil {
		return c, err
	}

	if err = setTCPSocketTimeoutFlags(c); err != nil {
		return c, err
	}

	return &timeoutConn{conn: c, readTimeout: t.readTimeout, writeTimeout: t.writeTimeout}, nil
}

func setTCPSocketTimeoutFlags(conn net.Conn) error {
	var err error
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("Bad conn type: %T", conn)
	}
	if err = tcp.SetKeepAlive(true); err != nil {
		return err
	}
	file, err := tcp.File()
	if err != nil {
		return err
	}
	fd := int(file.Fd())

	if err = os.NewSyscallError("setsockopt", syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, idleKeepAliveStartSeconds)); err != nil {
		return err
	}
	if err = os.NewSyscallError("setsockopt", syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, keepAliveProbeCnt)); err != nil {
		return err
	}
	if err = os.NewSyscallError("setsockopt", syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, keepAliveProbeIntervalSeconds)); err != nil {
		return err
	}
	return nil
}

func (t timeoutDialer) DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error) {
	// If we don't have any timeouts set, just return a normal connection
	if t.readTimeout == 0 && t.writeTimeout == 0 {
		return t.netDialTimeout(network, address, timeout)
	}

	// Otherwise we want a timeoutConn to handle the read and write deadlines for us.
	c, err := t.netDialTimeout(network, address, timeout)
	if err != nil || c == nil {
		return c, err
	}

	return &timeoutConn{conn: c, readTimeout: t.readTimeout, writeTimeout: t.writeTimeout}, nil
}

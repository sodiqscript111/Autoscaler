package redisx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr     string
	Password string
	DB       int
	Timeout  time.Duration
}

type Client struct {
	config Config
}

func NewClient(config Config) *Client {
	if config.Addr == "" {
		config.Addr = "localhost:6379"
	}
	if config.Timeout <= 0 {
		config.Timeout = 500 * time.Millisecond
	}

	return &Client{config: config}
}

func (c *Client) Ping(ctx context.Context) error {
	conn, reader, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := writeCommand(conn, "PING"); err != nil {
		return err
	}

	return readSimpleResponse(reader)
}

func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	conn, reader, err := c.open(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	ttlMillis := strconv.FormatInt(ttl.Milliseconds(), 10)
	if ttl <= 0 {
		if err := writeCommand(conn, "SET", key, value); err != nil {
			return err
		}
	} else {
		if err := writeCommand(conn, "SET", key, value, "PX", ttlMillis); err != nil {
			return err
		}
	}

	return readSimpleResponse(reader)
}

func (c *Client) open(ctx context.Context) (net.Conn, *bufio.Reader, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", c.config.Addr)
	if err != nil {
		return nil, nil, err
	}

	deadline := time.Now().Add(c.config.Timeout)
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return nil, nil, err
	}

	reader := bufio.NewReader(conn)

	if c.config.Password != "" {
		if err := writeCommand(conn, "AUTH", c.config.Password); err != nil {
			conn.Close()
			return nil, nil, err
		}
		if err := readSimpleResponse(reader); err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("auth: %w", err)
		}
	}

	if c.config.DB > 0 {
		if err := writeCommand(conn, "SELECT", strconv.Itoa(c.config.DB)); err != nil {
			conn.Close()
			return nil, nil, err
		}
		if err := readSimpleResponse(reader); err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("select db: %w", err)
		}
	}

	return conn, reader, nil
}

func writeCommand(conn net.Conn, args ...string) error {
	var builder strings.Builder
	builder.WriteString("*")
	builder.WriteString(strconv.Itoa(len(args)))
	builder.WriteString("\r\n")

	for _, arg := range args {
		builder.WriteString("$")
		builder.WriteString(strconv.Itoa(len(arg)))
		builder.WriteString("\r\n")
		builder.WriteString(arg)
		builder.WriteString("\r\n")
	}

	_, err := conn.Write([]byte(builder.String()))
	return err
}

func readSimpleResponse(reader *bufio.Reader) error {
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimRight(line, "\r\n")

	if line == "" {
		return fmt.Errorf("empty redis response")
	}

	switch line[0] {
	case '+', ':':
		return nil
	case '-':
		return errors.New(strings.TrimPrefix(line, "-"))
	case '$':
		length, err := strconv.Atoi(strings.TrimPrefix(line, "$"))
		if err != nil {
			return err
		}
		if length < 0 {
			return nil
		}
		buf := make([]byte, length+2)
		_, err = io.ReadFull(reader, buf)
		return err
	default:
		return fmt.Errorf("unexpected redis response: %s", line)
	}
}

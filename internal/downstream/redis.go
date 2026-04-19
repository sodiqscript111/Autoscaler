package downstream

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

type RedisConfig struct {
	Name     string
	Addr     string
	Password string
	DB       int
	Policy   Policy
	Interval time.Duration
	Timeout  time.Duration
}

type RedisChecker struct {
	monitor *Monitor
	config  RedisConfig
}

func NewRedisChecker(monitor *Monitor, config RedisConfig) *RedisChecker {
	if config.Name == "" {
		config.Name = "redis"
	}
	if config.Addr == "" {
		config.Addr = "localhost:6379"
	}
	if config.Interval <= 0 {
		config.Interval = time.Second
	}
	if config.Timeout <= 0 {
		config.Timeout = 500 * time.Millisecond
	}
	if config.Policy == "" {
		config.Policy = PolicyProtective
	}

	return &RedisChecker{
		monitor: monitor,
		config:  config,
	}
}

func (c *RedisChecker) Start(ctx context.Context) {
	if c == nil || c.monitor == nil {
		return
	}

	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()

	c.checkOnce(ctx)
	for {
		select {
		case <-ticker.C:
			c.checkOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (c *RedisChecker) checkOnce(ctx context.Context) {
	started := time.Now()
	err := pingRedis(ctx, c.config)

	sample := Sample{
		Name:      c.config.Name,
		Kind:      KindRedis,
		Operation: "ping",
		Policy:    c.config.Policy,
		Duration:  time.Since(started),
		Success:   err == nil,
		Timestamp: time.Now(),
	}
	if err != nil {
		sample.Error = err.Error()
	}

	c.monitor.Record(sample)
}

func pingRedis(ctx context.Context, config RedisConfig) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", config.Addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	deadline := time.Now().Add(config.Timeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)

	if config.Password != "" {
		if err := writeRedisCommand(conn, "AUTH", config.Password); err != nil {
			return err
		}
		if err := readRedisResponse(reader); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
	}

	if config.DB > 0 {
		if err := writeRedisCommand(conn, "SELECT", strconv.Itoa(config.DB)); err != nil {
			return err
		}
		if err := readRedisResponse(reader); err != nil {
			return fmt.Errorf("select db: %w", err)
		}
	}

	if err := writeRedisCommand(conn, "PING"); err != nil {
		return err
	}
	if err := readRedisResponse(reader); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	return nil
}

func writeRedisCommand(conn net.Conn, args ...string) error {
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

func readRedisResponse(reader *bufio.Reader) error {
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

package zookeeper

import (
	"crypto/tls"
	"fmt"
	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	mwtls "github.com/coredns/coredns/plugin/pkg/tls"
	"github.com/coredns/coredns/plugin/pkg/upstream"
	"github.com/go-zookeeper/zk"
	"path/filepath"
	"time"
)

const (
	priority  = 10  // default priority when nothing is set
	ttl       = 300 // default ttl when nothing is set
	zkTimeout = 5 * time.Second
)

func init() { plugin.Register(Name, setupZookeeper) }

func setupZookeeper(c *caddy.Controller) error {
	z, err := zkParse(c)
	if err != nil {
		return plugin.Error(Name, err)
	}

	c.OnShutdown(z.OnShutdown)

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		z.Next = next
		return z
	})

	return nil
}

func zkParse(c *caddy.Controller) (*Zookeeper, error) {
	config := dnsserver.GetConfig(c)
	zookeeper := Zookeeper{PathPrefix: "skydns"}
	var (
		tlsConfig *tls.Config
		err       error
		endpoints = []string{defaultEndpoint}
		username  string
		password  string
	)

	zookeeper.Upstream = upstream.New()

	if c.Next() {
		zookeeper.Zones = plugin.OriginsFromArgsOrServerBlock(c.RemainingArgs(), c.ServerBlockKeys)
		for c.NextBlock() {
			switch c.Val() {
			case "fallthrough":
				zookeeper.Fall.SetZonesFromArgs(c.RemainingArgs())
			case "debug":
				/* it is a noop now */
			case "path":
				if !c.NextArg() {
					return &Zookeeper{}, c.ArgErr()
				}
				zookeeper.PathPrefix = c.Val()
			case "endpoint":
				args := c.RemainingArgs()
				if len(args) == 0 {
					return &Zookeeper{}, c.ArgErr()
				}
				endpoints = args
			case "upstream":
				// remove soon
				c.RemainingArgs()
			case "tls": // cert key cacertfile
				args := c.RemainingArgs()
				for i := range args {
					if !filepath.IsAbs(args[i]) && config.Root != "" {
						args[i] = filepath.Join(config.Root, args[i])
					}
				}
				tlsConfig, err = mwtls.NewTLSConfigFromArgs(args...)
				if err != nil {
					return &Zookeeper{}, err
				}
			case "credentials":
				args := c.RemainingArgs()
				if len(args) == 0 {
					return &Zookeeper{}, c.ArgErr()
				}
				if len(args) != 2 {
					return &Zookeeper{}, c.Errf("credentials requires 2 arguments, username and password")
				}
				username, password = args[0], args[1]
			default:
				if c.Val() != "}" {
					return &Zookeeper{}, c.Errf("unknown property '%s'", c.Val())
				}
			}
		}
		conn, err := newZookeeperClient(endpoints, tlsConfig, username, password)
		if err != nil {
			return &Zookeeper{}, err
		}
		zookeeper.conn = conn
		return &zookeeper, nil
	}
	return &Zookeeper{}, nil
}

func newZookeeperClient(endpoints []string, cc *tls.Config, username, password string) (*zk.Conn, error) {
	c, _, err := zk.Connect(endpoints, zkTimeout) //*10)
	if err != nil {
		fmt.Println("failed to get CPU utilization")
		return nil, err
	}
	return c, nil
}

const defaultEndpoint = "127.0.0.1"

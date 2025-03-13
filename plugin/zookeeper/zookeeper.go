// Package zookeeper implements a plugin that returns details about znodes
package zookeeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/etcd/msg"
	"github.com/coredns/coredns/plugin/pkg/fall"
	"github.com/coredns/coredns/plugin/pkg/upstream"
	"github.com/coredns/coredns/request"
	"github.com/go-zookeeper/zk"
	"github.com/miekg/dns"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"strings"
	"time"
)

var errKeyNotFound = errors.New("key not found")

const Name = "zookeeper"

// Zookeeper is a plugin that returns data associated with a znode
type Zookeeper struct {
	Next       plugin.Handler
	Fall       fall.F
	Zones      []string
	conn       *zk.Conn
	PathPrefix string
	Upstream   *upstream.Upstream
}

// Serial returns the serial number to use.
func (z *Zookeeper) Serial(state request.Request) uint32 {
	return uint32(time.Now().Unix())
}

// MinTTL returns the minimal TTL.
func (z *Zookeeper) MinTTL(state request.Request) uint32 {
	return 30
}

type Kv struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// Services implements the ServiceBackend interface.
func (z *Zookeeper) Services(ctx context.Context, state request.Request, exact bool, opt plugin.Options) (services []msg.Service, err error) {
	services, err = z.Records(ctx, state, exact)
	if err != nil {
		return
	}

	services = msg.Group(services)
	return
}

// Reverse implements the ServiceBackend interface.
func (z *Zookeeper) Reverse(ctx context.Context, state request.Request, exact bool, opt plugin.Options) (services []msg.Service, err error) {
	return z.Services(ctx, state, exact, opt)
}

// Lookup implements the ServiceBackend interface.
func (z *Zookeeper) Lookup(ctx context.Context, state request.Request, name string, typ uint16) (*dns.Msg, error) {
	return z.Upstream.Lookup(ctx, state, name, typ)
}

// IsNameError implements the ServiceBackend interface.
func (z *Zookeeper) IsNameError(err error) bool {
	return err == errKeyNotFound
}

// Records looks up records in etcd. If exact is true, it will lookup just this
// name. This is used when find matches when completing SRV lookups for instance.
func (z *Zookeeper) Records(ctx context.Context, state request.Request, exact bool) ([]msg.Service, error) {
	name := state.Name()

	path, star := msg.PathWithWildcard(name, z.PathPrefix)
	r, err := z.get(ctx, path, !exact)
	if err != nil {
		return nil, err
	}
	segments := strings.Split(msg.Path(name, z.PathPrefix), "/")
	return z.loopNodes(r, segments, star, state.QType())
}

func (z *Zookeeper) get(ctx context.Context, path string, recursive bool) ([]*Kv, error) {
	r := make([]*Kv, 0)
	if recursive {
		children, _, err2 := z.conn.Children(path)
		if err2 != nil {
			return nil, err2
		}
		if len(children) == 0 {
			path = strings.TrimSuffix(path, "/")
			v, _, err3 := z.conn.Get(path)
			if err3 != nil {
				return nil, errKeyNotFound
			}
			return []*Kv{{Key: path, Value: v}}, nil
		}
		for _, childKey := range children {
			p := path + "/" + childKey
			value, _, err3 := z.conn.Get(p)
			r = append(r, &Kv{Key: p, Value: value})
			if err3 != nil {
				continue
			}
		}
		return r, nil
	}
	v, _, err3 := z.conn.Get(path)
	if err3 != nil {
		return nil, errKeyNotFound
	}
	return []*Kv{{Key: path, Value: v}}, nil
}

func (z *Zookeeper) loopNodes(kv []*Kv, nameParts []string, star bool, qType uint16) (sx []msg.Service, err error) {
	bx := make(map[msg.Service]struct{})
Nodes:
	for _, n := range kv {
		if star {
			s := n.Key
			keyParts := strings.Split(s, "/")
			for i, n := range nameParts {
				if i > len(keyParts)-1 {
					// name is longer than key
					continue Nodes
				}
				if n == "*" || n == "any" {
					continue
				}
				if keyParts[i] != n {
					continue Nodes
				}
			}
		}
		serv := new(msg.Service)
		if err := json.Unmarshal(n.Value, serv); err != nil {
			return nil, fmt.Errorf("%s: %s", n.Key, err.Error())
		}
		serv.Key = n.Key
		if _, ok := bx[*serv]; ok {
			continue
		}
		bx[*serv] = struct{}{}

		//serv.TTL = z.TTL(n, serv)
		if serv.Priority == 0 {
			serv.Priority = priority
		}

		if shouldInclude(serv, qType) {
			sx = append(sx, *serv)
		}
	}
	return sx, nil
}

// TTL returns the smaller of the etcd TTL and the service's
// TTL. If neither of these are set (have a zero value), a default is used.
func (z *Zookeeper) TTL(kv *mvccpb.KeyValue, serv *msg.Service) uint32 {
	etcdTTL := uint32(kv.Lease)

	if etcdTTL == 0 && serv.TTL == 0 {
		return ttl
	}
	if etcdTTL == 0 {
		return serv.TTL
	}
	if serv.TTL == 0 {
		return etcdTTL
	}
	if etcdTTL < serv.TTL {
		return etcdTTL
	}
	return serv.TTL
}

// shouldInclude returns true if the service should be included in a list of records, given the qType. For all the
// currently supported lookup types, the only one to allow for an empty Host field in the service are TXT records
// which resolve directly.  If a TXT record is being resolved by CNAME, then we expect the Host field to have a
// value while the TXT field will be empty.
func shouldInclude(serv *msg.Service, qType uint16) bool {
	return (qType == dns.TypeTXT && serv.Text != "") || serv.Host != ""
}

func (z *Zookeeper) OnShutdown() error {
	z.conn.Close()
	return nil
}

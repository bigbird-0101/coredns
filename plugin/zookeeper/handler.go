package zookeeper

import (
	"context"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"

	"github.com/miekg/dns"
)

// ServeDNS implements the plugin.Handler interface.
func (z *Zookeeper) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	opt := plugin.Options{}
	state := request.Request{W: w, Req: r}

	zone := plugin.Zones(z.Zones).Matches(state.Name())
	if zone == "" {
		return plugin.NextOrFailure(z.Name(), z.Next, ctx, w, r)
	}

	var (
		records, extra []dns.RR
		truncated      bool
		err            error
	)

	switch state.QType() {
	case dns.TypeA:
		records, truncated, err = plugin.A(ctx, z, zone, state, nil, opt)
	case dns.TypeAAAA:
		records, truncated, err = plugin.AAAA(ctx, z, zone, state, nil, opt)
	case dns.TypeTXT:
		records, truncated, err = plugin.TXT(ctx, z, zone, state, nil, opt)
	case dns.TypeCNAME:
		records, err = plugin.CNAME(ctx, z, zone, state, opt)
	case dns.TypePTR:
		records, err = plugin.PTR(ctx, z, zone, state, opt)
	case dns.TypeMX:
		records, extra, err = plugin.MX(ctx, z, zone, state, opt)
	case dns.TypeSRV:
		records, extra, err = plugin.SRV(ctx, z, zone, state, opt)
	case dns.TypeSOA:
		records, err = plugin.SOA(ctx, z, zone, state, opt)
	case dns.TypeNS:
		if state.Name() == zone {
			records, extra, err = plugin.NS(ctx, z, zone, state, opt)
			break
		}
		fallthrough
	default:
		// Do a fake A lookup, so we can distinguish between NODATA and NXDOMAIN
		_, _, err = plugin.A(ctx, z, zone, state, nil, opt)
	}
	if err != nil && z.IsNameError(err) {
		if z.Fall.Through(state.Name()) {
			return plugin.NextOrFailure(z.Name(), z.Next, ctx, w, r)
		}
		// Make err nil when returning here, so we don't log spam for NXDOMAIN.
		return plugin.BackendError(ctx, z, zone, dns.RcodeNameError, state, nil /* err */, opt)
	}
	if err != nil {
		return plugin.BackendError(ctx, z, zone, dns.RcodeServerFailure, state, err, opt)
	}

	if len(records) == 0 {
		return plugin.BackendError(ctx, z, zone, dns.RcodeSuccess, state, err, opt)
	}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Truncated = truncated
	m.Authoritative = true
	m.Answer = records
	m.Extra = extra

	w.WriteMsg(m)
	return dns.RcodeSuccess, nil
}

// Name implements the Handler interface.
func (z *Zookeeper) Name() string { return Name }

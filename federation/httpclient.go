// Copyright (c) 2024 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package federation

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"

	"go.mau.fi/util/exhttp"
)

// ServerResolvingTransport is an http.RoundTripper that resolves Matrix server names before sending requests.
// It only allows requests using the "matrix-federation" scheme.
type ServerResolvingTransport struct {
	ResolveOpts *ResolveServerNameOpts
	Transport   *http.Transport
	DialFunc    exhttp.DialerFunc

	cache ResolutionCache

	resolveLocks     map[string]*sync.Mutex
	resolveLocksLock sync.Mutex
}

func NewServerResolvingTransport(cache ResolutionCache, dialer exhttp.DialerFunc, settings exhttp.ClientSettings) *ServerResolvingTransport {
	if cache == nil {
		cache = NewInMemoryCache()
	}
	srt := &ServerResolvingTransport{
		resolveLocks: make(map[string]*sync.Mutex),
		cache:        cache,
		DialFunc:     dialer,
	}
	srt.Transport = settings.WithDial(srt.DialContext).Configure(&http.Transport{})
	return srt
}

var _ http.RoundTripper = (*ServerResolvingTransport)(nil)

func (srt *ServerResolvingTransport) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	addrs, ok := ctx.Value(contextKeyIPPort).([]string)
	if !ok {
		return nil, fmt.Errorf("no IP:port in context")
	}
	return srt.DialFunc(ctx, network, addrs[0])
}

func (srt *ServerResolvingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != "matrix-federation" {
		return nil, fmt.Errorf("unsupported scheme: %s", request.URL.Scheme)
	}
	resolved, err := srt.resolve(request.Context(), request.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve server name: %w", err)
	}
	request = request.WithContext(context.WithValue(request.Context(), contextKeyIPPort, resolved.IPPort))
	request.URL.Scheme = "https"
	request.URL.Host = resolved.HostHeader
	request.Host = resolved.HostHeader
	return srt.Transport.RoundTrip(request)
}

func (srt *ServerResolvingTransport) resolve(ctx context.Context, serverName string) (*ResolvedServerName, error) {
	srt.resolveLocksLock.Lock()
	lock, ok := srt.resolveLocks[serverName]
	if !ok {
		lock = &sync.Mutex{}
		srt.resolveLocks[serverName] = lock
	}
	srt.resolveLocksLock.Unlock()

	lock.Lock()
	defer lock.Unlock()
	res, err := srt.cache.LoadResolution(serverName)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache: %w", err)
	} else if res != nil {
		return res, nil
	} else if res, err = ResolveServerName(ctx, serverName, srt.ResolveOpts); err != nil {
		return nil, err
	} else {
		srt.cache.StoreResolution(res)
		return res, nil
	}
}

func NoopDNSFilter(ips []net.IP) []net.IP {
	return ips
}

func DefaultDNSFilter(ips []net.IP) []net.IP {
	return slices.DeleteFunc(ips, func(ip net.IP) bool {
		return ip.IsPrivate() || ip.IsLoopback()
	})
}

func (c *Client) dnsResolve(ctx context.Context, network, host string) ([]net.IP, error) {
	results, err := c.DNS.LookupIP(ctx, network, host)
	if err != nil {
		return nil, err
	}
	return c.DNSFilter(results), nil
}

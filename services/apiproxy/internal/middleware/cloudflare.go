package middleware

import (
	"context"
	"net"
	"net/http"
)

type contextKey string

const RealIPKey contextKey = "real-ip"

var cloudflareIPRanges = []string{
	"173.245.48.0/20",
	"103.21.244.0/22",
	"103.22.200.0/22",
	"103.31.4.0/22",
	"141.101.64.0/18",
	"108.162.192.0/18",
	"190.93.240.0/20",
	"188.114.96.0/20",
	"197.234.240.0/22",
	"198.41.128.0/17",
	"162.158.0.0/15",
	"104.16.0.0/13",
	"104.24.0.0/14",
	"172.64.0.0/13",
	"131.0.72.0/22",
}

type CloudflareMiddleware struct {
	useHeaders    bool
	requireSource bool
	cloudflareIPs []*net.IPNet
}

func NewCloudflareMiddleware(useHeaders, requireSource bool) (*CloudflareMiddleware, error) {
	cm := &CloudflareMiddleware{
		useHeaders:    useHeaders,
		requireSource: requireSource,
		cloudflareIPs: make([]*net.IPNet, 0, len(cloudflareIPRanges)),
	}

	for _, cidr := range cloudflareIPRanges {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, err
		}
		cm.cloudflareIPs = append(cm.cloudflareIPs, ipNet)
	}

	return cm, nil
}

func (cm *CloudflareMiddleware) isCloudflareIP(ip net.IP) bool {
	for _, ipNet := range cm.cloudflareIPs {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

func (cm *CloudflareMiddleware) extractRealIP(r *http.Request) string {
	if cm.useHeaders {
		if cfIP := r.Header.Get("CF-Connecting-IP"); cfIP != "" {
			return cfIP
		}
	}

	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

func (cm *CloudflareMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cm.requireSource {
			remoteIP, _, _ := net.SplitHostPort(r.RemoteAddr)
			remoteIPParsed := net.ParseIP(remoteIP)

			if remoteIPParsed != nil && !cm.isCloudflareIP(remoteIPParsed) {
				http.Error(w, "Forbidden: Invalid request source", http.StatusForbidden)
				return
			}
		}

		realIP := cm.extractRealIP(r)
		ctx := context.WithValue(r.Context(), RealIPKey, realIP)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetRealIP(r *http.Request) string {
	if ip, ok := r.Context().Value(RealIPKey).(string); ok {
		return ip
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

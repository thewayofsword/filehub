package main

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// discoveryPort is the fixed UDP broadcast port every FileHub instance
// listens on to find each other on the same subnet. It is independent of
// the HTTP port (which may be changed via flag).
const discoveryPort = 36774

const (
	announceInterval = 2 * time.Second
	peerExpireAfter  = 8 * time.Second
)

// Announce is the UDP broadcast payload.
type Announce struct {
	Type     string `json:"t"` // "announce"
	Hostname string `json:"h"`
	Port     int    `json:"port"` // HTTP port
	Version  string `json:"v"`
}

// Peer is a discovered or manually-added FileHub instance.
type Peer struct {
	Hostname string    `json:"hostname"`
	IP       string    `json:"ip"`
	Port     int       `json:"port"`
	Version  string    `json:"version"`
	Manual   bool      `json:"manual"`
	LastSeen time.Time `json:"-"`
}

// Discovery broadcasts our presence and collects peers. Besides UDP
// broadcast discovery it also keeps a persistent list of manually-added
// peers — essential for cross-network / Tailscale scenarios where broadcast
// discovery cannot reach the other side.
type Discovery struct {
	mu          sync.RWMutex
	peers       map[string]*Peer // broadcast-discovered, keyed by IP
	manual      map[string]*Peer // manually-added, keyed by ip:port
	persistPath string           // JSON file backing manual peers
	conn        *net.UDPConn
	hostname    string
	version     string
	port        int
	stop        chan struct{}
}

func NewDiscovery(hostname, version string, port int, persistPath string) *Discovery {
	return &Discovery{
		peers:       make(map[string]*Peer),
		manual:      make(map[string]*Peer),
		persistPath: persistPath,
		hostname:    hostname,
		version:     version,
		port:        port,
		stop:        make(chan struct{}),
	}
}

func (d *Discovery) Start() error {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: discoveryPort})
	if err != nil {
		return err
	}
	d.conn = conn
	go d.listenLoop()
	go d.broadcastLoop()
	go d.cleanupLoop()
	return nil
}

func (d *Discovery) listenLoop() {
	buf := make([]byte, 2048)
	for {
		n, remote, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var a Announce
		if err := json.Unmarshal(buf[:n], &a); err != nil {
			continue
		}
		if a.Type != "announce" {
			continue
		}
		src := remote.IP.String()
		if isLocalIP(src) {
			continue // ignore our own broadcasts
		}
		d.mu.Lock()
		p := d.peers[src]
		if p == nil {
			p = &Peer{IP: src}
			d.peers[src] = p
		}
		p.Hostname = a.Hostname
		p.Port = a.Port
		p.Version = a.Version
		p.LastSeen = time.Now()
		d.mu.Unlock()
	}
}

func (d *Discovery) broadcastLoop() {
	d.send()
	t := time.NewTicker(announceInterval)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-t.C:
			d.send()
		}
	}
}

func (d *Discovery) send() {
	msg, err := json.Marshal(Announce{
		Type:     "announce",
		Hostname: d.hostname,
		Port:     d.port,
		Version:  d.version,
	})
	if err != nil {
		return
	}
	addrs, err := broadcastAddrs()
	if err != nil {
		return
	}
	for _, a := range addrs {
		_, _ = d.conn.WriteToUDP(msg, &net.UDPAddr{IP: a, Port: discoveryPort})
	}
}

func (d *Discovery) cleanupLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-t.C:
			now := time.Now()
			d.mu.Lock()
			for k, p := range d.peers {
				if now.Sub(p.LastSeen) > peerExpireAfter {
					delete(d.peers, k)
				}
			}
			d.mu.Unlock()
		}
	}
}

// LoadManual reads the persisted manual-peer list at startup.
func (d *Discovery) LoadManual() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.persistPath == "" {
		return nil
	}
	data, err := os.ReadFile(d.persistPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var list []Peer
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	for i := range list {
		p := list[i]
		p.Manual = true
		d.manual[peerKey(p.IP, p.Port)] = &p
	}
	return nil
}

// AddManual registers a manually-added peer and persists it.
func (d *Discovery) AddManual(ip string, port int, hostname, version string) Peer {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := &Peer{
		Hostname: hostname,
		IP:       ip,
		Port:     port,
		Version:  version,
		Manual:   true,
		LastSeen: time.Now(),
	}
	d.manual[peerKey(ip, port)] = p
	d.saveManualLocked()
	return *p
}

// RemoveManual deletes a manually-added peer. Returns false if not found.
func (d *Discovery) RemoveManual(ip string, port int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	k := peerKey(ip, port)
	if _, ok := d.manual[k]; !ok {
		return false
	}
	delete(d.manual, k)
	d.saveManualLocked()
	return true
}

func (d *Discovery) saveManualLocked() {
	if d.persistPath == "" {
		return
	}
	list := make([]Peer, 0, len(d.manual))
	for _, p := range d.manual {
		list = append(list, *p)
	}
	sort.Slice(list, func(i, j int) bool { return peerKey(list[i].IP, list[i].Port) < peerKey(list[j].IP, list[j].Port) })
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(d.persistPath, data, 0o644)
}

func peerKey(ip string, port int) string {
	return net.JoinHostPort(ip, strconv.Itoa(port))
}

// Peers returns broadcast-discovered and manual peers merged, deduplicated
// by ip:port, and stably sorted by IP (numeric) then hostname.
func (d *Discovery) Peers() []Peer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	seen := make(map[string]bool)
	out := make([]Peer, 0, len(d.peers)+len(d.manual))
	add := func(p *Peer) {
		k := peerKey(p.IP, p.Port)
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, *p)
	}
	for _, p := range d.peers {
		add(p)
	}
	for _, p := range d.manual {
		add(p)
	}
	sort.Slice(out, func(i, j int) bool {
		c := compareIP(out[i].IP, out[j].IP)
		if c != 0 {
			return c < 0
		}
		return out[i].Hostname < out[j].Hostname
	})
	return out
}

// compareIP compares two IPv4 strings numerically, octet by octet.
// Returns -1/0/1; invalid IPs are treated as text to stay deterministic.
func compareIP(a, b string) int {
	ia := net.ParseIP(a)
	ib := net.ParseIP(b)
	if ia == nil || ib == nil {
		return bytes.Compare([]byte(a), []byte(b))
	}
	ia = ia.To4()
	ib = ib.To4()
	if ia == nil || ib == nil {
		return bytes.Compare([]byte(a), []byte(b))
	}
	return bytes.Compare(ia, ib)
}

// broadcastAddrs returns the directed broadcast address of each up IPv4
// interface plus the limited broadcast 255.255.255.255.
func broadcastAddrs() ([]net.IP, error) {
	var addrs []net.IP
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagBroadcast == 0 {
			continue
		}
		iaddrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range iaddrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			mask := ipnet.Mask
			bcast := make(net.IP, 4)
			for i := 0; i < 4; i++ {
				bcast[i] = ip4[i] | ^mask[i]
			}
			addrs = append(addrs, bcast)
		}
	}
	addrs = append(addrs, net.IPv4(255, 255, 255, 255))
	return addrs, nil
}

// localIPList returns the IPv4 addresses of all up, non-loopback interfaces.
func localIPList() []string {
	var out []string
	ifaces, _ := net.Interfaces()
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := ifc.Addrs()
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				out = append(out, ip4.String())
			}
		}
	}
	return out
}

func isLocalIP(ip string) bool {
	for _, l := range localIPList() {
		if l == ip {
			return true
		}
	}
	return false
}

func primaryLocalIP() string {
	if l := localIPList(); len(l) > 0 {
		return l[0]
	}
	return "127.0.0.1"
}

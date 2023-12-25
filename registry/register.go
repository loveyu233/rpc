package registry

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type GoRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_gorpc_/registry"
	defaultTimeout = time.Minute * 5
)

func NewGoRegister(timeout time.Duration) *GoRegistry {
	return &GoRegistry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultGoRegister = NewGoRegister(defaultTimeout)

func (g *GoRegistry) putServer(addr string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.servers[addr]
	if s == nil {
		g.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		s.start = time.Now()
	}
}

func (g *GoRegistry) aliveServers() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var alive []string
	for addr, s := range g.servers {
		if g.timeout == 0 || s.start.Add(g.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(g.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

func (g *GoRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-GORPC-Servers", strings.Join(g.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-GORPC-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		g.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (g *GoRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, g)
	log.Println("RPC 注册中心地址:", registryPath)
}

func HandleHTTP() {
	DefaultGoRegister.HandleHTTP(defaultPath)
}

func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "发送心跳到注册表", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-GORPC-Server", addr)
	resp, err := httpClient.Do(req)
	fmt.Println("心跳发送返回: " + resp.Status)
	if err != nil {
		log.Println("RPC 服务: 心跳发送失败", err)
		return err
	}
	return nil
}

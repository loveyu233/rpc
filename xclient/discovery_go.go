package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type GoRegistryDiscovery struct {
	*MultiServerDiscovery               // 能力复用
	registry              string        // 注册中心的地址
	timeout               time.Duration // 服务列表的过期时间
	lastUpdate            time.Time     // 代表最后从注册中心更新服务列表的时间，默认 10s 过期，即 10s 之后，需要从注册中心更新新的列表
}

const defaultUpdateTimeout = time.Second * 10

func NewGoRegistryDiscovery(registerAddr string, timeout time.Duration) *GoRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &GoRegistryDiscovery{
		MultiServerDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:             registerAddr,
		timeout:              timeout,
	}
	return d
}

func (g *GoRegistryDiscovery) Update(servers []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.servers = servers
	g.lastUpdate = time.Now()
	return nil
}

func (g *GoRegistryDiscovery) Refresh() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastUpdate.Add(g.timeout).After(time.Now()) {
		return nil
	}
	log.Println("RPC 从注册中心刷新服务", g.registry)
	resp, err := http.Get(g.registry)
	if err != nil {
		log.Println("RPC 从注册中心刷新服务失败:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-GORPC-Servers"), ",")
	g.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			g.servers = append(g.servers, strings.TrimSpace(server))
		}
	}
	g.lastUpdate = time.Now()
	return nil
}

func (g *GoRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := g.Refresh(); err != nil {
		return "", err
	}
	return g.MultiServerDiscovery.Get(mode)
}

func (g *GoRegistryDiscovery) GetAll() ([]string, error) {
	if err := g.Refresh(); err != nil {
		return nil, err
	}
	return g.MultiServerDiscovery.GetAll()
}

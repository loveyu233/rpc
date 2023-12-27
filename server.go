package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"rpc/codec"
	"strings"
	"sync"
	"time"
)

// MagicNumber 用来标识当前请求
const MagicNumber = 0x3bef5c

// Option 消息的编解码方式
type Option struct {
	MagicNumber    int           // 请求标志
	CodecType      codec.Type    // 编码方式
	ConnectTimeout time.Duration // 连接超时
	HandleTimeout  time.Duration // 处理超时
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server /*
/*
| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的:
| Option | Header1 | Body1 | Header2 | Body2 | ...
*/
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return new(Server)
}

// DefaultServer 默认的服务实例
var DefaultServer = NewServer()

// Accept 接受连接,提供请求服务连接
func (s *Server) Accept(lis net.Listener) {
	for {
		accept, err := lis.Accept()
		if err != nil {
			log.Println("RPC 服务器接受错误: ", err)
			return
		}
		// 开启子协程处理，处理过程交给了 ServerConn 方法
		go s.ServeConn(accept)
	}
}

// Accept 默认服务的请求连接
/*
	lis, _ := net.Listen("tcp", ":9999")
	rpc.Accept(lis)
*/
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// ServeConn 连接处理
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	var opt Option
	// 反序列化得到 Option 实例
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("RPC 服务器：选项错误: ", err)
		return
	}
	// 检查 MagicNumber 和 CodeType 的值是否正确
	if opt.MagicNumber != MagicNumber {
		log.Printf("RPC 服务器：无效的标识 %x", opt.MagicNumber)
		return
	}
	// 根据 CodeType 得到对应的消息编解码器，接下来的处理交给 serverCodec
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("RPC 服务器：无效的编解码器类型%s", opt.CodecType)
		return
	}
	s.serveCodec(f(conn), &opt)
}

// 发生错误响应时的argv占位符
var invalidRequest = struct{}{}

func (s *Server) serveCodec(cc codec.Codec, opt *Option) {
	// 确保发送完整的回复
	sending := new(sync.Mutex)
	// 等到所有请求都处理完毕
	wg := new(sync.WaitGroup)
	for {
		// 读取请求 readRequest
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				// 如果请求有错误则直接结束
				break
			}
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h           *codec.Header // 请求头
	argv, reply reflect.Value // 请求的参数和答复
	methodType  *MethodType
	svc         *Service
}

func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	header, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: header}

	req.svc, req.methodType, err = s.findService(header.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.methodType.NewArgv()
	req.reply = req.methodType.NewReply()
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("RPC 服务器：读取 argv 错误:", err)
	}
	return req, nil
}

// readRequestHeader 读取请求头信息
func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("RPC 服务器：读取标头错误:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (s *Server) findService(serviceMethod string) (svc *Service, mtype *MethodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("RPC 服务: 服务/方法请求格式不对" + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New(":RPC 服务: 找不到服务:" + serviceName)
		return
	}
	svc = svci.(*Service)
	mtype = svc.Method[methodName]
	if mtype == nil {
		err = errors.New("RPC 服务: 找不到方法:" + methodName)
		return
	}
	return
}

func (s *Server) Register(val interface{}) error {
	newService := NewService(val)
	if _, dup := s.serviceMap.LoadOrStore(newService.Name, newService); dup {
		return errors.New("RPC 服务已定义:" + newService.Name)
	}
	return nil
}

func Register(val interface{}) error {
	return DefaultServer.Register(val)
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("RPC 服务器：写入响应错误:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.Call(req.methodType, req.argv, req.reply)
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			return
		}
		s.sendResponse(cc, req.h, req.reply.Interface(), sending)
		sent <- struct{}{}
	}()
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	/*
		需要确保 sendResponse 仅调用一次，因此将整个过程拆分为 called 和 sent 两个阶段，在这段代码中只会发生如下两种情况
			1. called 信道接收到消息，代表处理没有超时，继续执行 sendResponse。
			2. time.After() 先于 called 接收到消息，说明处理已经超时，called 和 sent 都将被阻塞。在 case <-time.After(timeout) 处调用 sendResponse。
	*/
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("RPC 服务端请求处理超时")
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

const (
	Connected        = "200 Connected to Go RPC"
	DefaultRPCPath   = "/_gorpc_"
	DefaultDebugPath = "/debug/gorpc"
)

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("RPC Hijack", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+Connected+"\n\n")
	s.ServeConn(conn)
}

func (s *Server) HandleHTTP() {
	http.Handle(DefaultRPCPath, s)
	http.Handle(DefaultDebugPath, debugHTTP{s})
	log.Println("RPC 服务debug路径:", DefaultDebugPath)
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}

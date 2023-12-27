package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"rpc/codec"
	"strings"
	"sync"
	"time"
)

// Call 为了支持异步调用，Call 结构体中添加了一个字段 Done，Done 的类型是 chan *Call，当调用结束时，会调用 call.done() 通知调用方
type Call struct {
	Seq           uint64
	ServiceMethod string      // 格式:<service>.<method>
	Args          interface{} // 函数的参数
	Reply         interface{} // 函数的返回
	Error         error       // 返回的错误
	Done          chan *Call  //  当调用结束时，会调用 call.done() 通知调用方
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec // 消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应
	opt      *Option
	sending  sync.Mutex       // 互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆
	header   codec.Header     // 每个请求的消息头，header 只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个，声明在 Client 结构体中可以复用
	mu       sync.Mutex       // 确保资源并发安全
	seq      uint64           // 用于给发送的请求编号，每个请求拥有唯一编号
	pending  map[uint64]*Call // 存储未处理完的请求，键是编号，值是 Call 实例
	closing  bool             // 用户主动关闭的
	shutdown bool             // 发生错误而中断连接
}

var ErrShutdown = errors.New("连接关闭")

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrShutdown
	}
	c.closing = true
	return c.cc.Close()
}

func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.shutdown && !c.closing
}

// registerCall 将参数 call 添加到 client.pending 中，并更新 client.seq
func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

// removeCall 根据 seq，从 client.pending 中移除对应的 call，并返回
func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

// terminateCalls 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call
func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			break
		}
		call := c.removeCall(h.Seq)
		switch {
		// 可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
		case call == nil:
			err = c.cc.ReadBody(nil)
		// 但服务端处理出错，即 h.Error 不为空
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		// 服务端处理正常，那么需要从 body 中读取 Reply 的值
		default:
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("读取body错误:" + err.Error())
			}
			call.done()
		}
	}
	c.terminateCalls(err)
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("无敌编解码类型 %s", opt.CodecType)
		log.Println("RPC 客户端 编解码错误:", err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("RPC 客户端 option编码错误:", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("opts长度只能为1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return DialTimeout(NewClient, network, address)
}

func (c *Client) send(call *Call) {
	c.sending.Lock()
	defer c.sending.Unlock()
	seq, err := c.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	c.header.ServiceMethod = call.ServiceMethod
	c.header.Seq = seq
	c.header.Error = ""
	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("RPC 客户端：通道无缓冲")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	return call
}

func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := c.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		c.removeCall(call.Seq)
		return errors.New("RPC 客户端:调用超时" + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

type ClientResult struct {
	Client *Client
	Err    error
}

type NewClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

// DialTimeout 实现了一个超时处理的外壳 dialTimeout，这个壳将 NewClient 作为入参
// 1. 将 net.Dial 替换为 net.DialTimeout，如果连接创建超时，将返回错误
// 2. 使用子协程执行 NewClient，执行完成后则通过信道 ch 发送结果，如果 time.After() 信道先接收到消息，则说明 NewClient 执行超时，返回错误
func DialTimeout(f NewClientFunc, netword, address string, opts ...*Option) (client *Client, err error) {
	options, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(netword, address, options.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan ClientResult)
	go func() {
		client, err := f(conn, options)
		ch <- ClientResult{client, err}
	}()
	if options.ConnectTimeout == 0 {
		result := <-ch
		return result.Client, result.Err
	}
	select {
	case <-time.After(options.ConnectTimeout):
		return nil, fmt.Errorf("rpc 客户端: 连接超时 %s", options.ConnectTimeout)
	case res := <-ch:
		return res.Client, res.Err
	}
}

func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", DefaultRPCPath))
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == Connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("意外的 HTTP 响应: " + resp.Status)
	}
	return nil, err
}

func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return DialTimeout(NewHTTPClient, network, address, opts...)
}

func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpcAddr地址错误:%s\n", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		return Dial(protocol, addr, opts...)
	}
}

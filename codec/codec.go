package codec

import "io"

// Header /*
/*
客户端发送的请求包括服务名 Arith，方法名 Multiply，参数 args 三个，
服务端的响应包括错误 error，返回值 reply 2 个。
将请求和响应中的参数和返回值抽象为 body，剩余的信息放在 header 中，
那么就可以抽象出数据结构 Header：
*/
type Header struct {
	ServiceMethod string // 服务名和方法名
	Seq           uint64 //请求的序号，也可以认为是某个请求的 ID，用来区分不同的请求
	Error         string // 错误信息
}

// Codec 抽象出对消息体进行编解码的接口 Codec，抽象出接口是为了实现不同的 Codec 实例
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}

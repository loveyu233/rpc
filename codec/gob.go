package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser // 连接实例
	buf  *bufio.Writer      // 防止阻塞而创建的带缓冲的 Writer,用来提升性能
	dec  *gob.Decoder       // 解码
	enc  *gob.Encoder       // 编码
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

func (g *GobCodec) ReadHeader(h *Header) error {
	return g.dec.Decode(h)
}

func (g *GobCodec) ReadBody(body interface{}) error {
	return g.dec.Decode(body)
}

func (g *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = g.buf.Flush()
		if err != nil {
			_ = g.Close()
		}
	}()
	if err := g.enc.Encode(h); err != nil {
		log.Println("gob header编码失败:", err)
		return err
	}
	if err := g.enc.Encode(body); err != nil {
		log.Println("gob body编码失败:", err)
		return err
	}
	return nil
}

func (g *GobCodec) Close() error {
	return g.conn.Close()
}

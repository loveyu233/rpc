package rpc

import (
	"fmt"
	"html/template"
	"net/http"
)

const debugText = `<html>
	<body>
	<title>GeeRPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $mtype := .Method}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$mtype.ArgType}}, {{$mtype.ReplyType}}) error</td>
			<td align=center>{{$mtype.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type debugHTTP struct {
	*Server
}
type debugService struct {
	Name   string
	Method map[string]*MethodType
}

func (d debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var services []debugService
	d.serviceMap.Range(func(namei, svci any) bool {
		svc := svci.(*Service)
		services = append(services, debugService{
			Name:   namei.(string),
			Method: svc.Method,
		})
		return true
	})
	err := debug.Execute(w, services)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: 执行模板时出错:", err.Error())
	}
}

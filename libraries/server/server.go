package server

import (
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/greymass/roborovski/libraries/encoding"
	"github.com/greymass/roborovski/libraries/enforce"
)

type Config struct {
	PrintSyncEvery uint32
	Debug          bool
	LIBOnly        bool
	ReadOnly       bool
	PrintTiming    bool
	PrintOnlyGTE   int
	AcceptHTTP     bool
}

func SocketListen(socket string) net.Listener {

	if strings.HasSuffix(socket, ".sock") {
		os.Remove(socket)
		unixListener, err := net.Listen("unix", socket)
		enforce.ENFORCE(err, "Listen failure (UNIX socket)", socket)
		err = os.Chmod(socket, 0777)
		enforce.ENFORCE(err)
		return unixListener
	} else {
		tcpListener, err := net.Listen("tcp", socket)
		enforce.ENFORCE(err, "Listen failure (TCP)", socket)
		return tcpListener
	}
}

func GetRequestParams(r *http.Request) (map[string]interface{}, error) {
	ret := make(map[string]interface{}, 0)
	var err error
	for k := range r.URL.Query() { // first try REST
		if len(r.URL.Query()[k]) == 1 {
			ret[k] = r.URL.Query()[k][0]
		} else {
			assn := make([]interface{}, len(r.URL.Query()[k]))
			for i, v := range r.URL.Query()[k] {
				assn[i] = v
			}
			ret[k] = assn
		}
	}
	if len(ret) == 0 { // try body
		rdecoder := encoding.JSONiter.NewDecoder(r.Body)
		defer r.Body.Close()
		err = rdecoder.Decode(&ret)
	}
	return ret, err
}

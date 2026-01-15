package server

import (
	"net/http"

	"github.com/greymass/roborovski/libraries/encoding"
)

func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := encoding.JSONiter.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(data)
}

func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

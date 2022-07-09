package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (server *httpServer) handleProduce(writer http.ResponseWriter, request *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(request.Body).Decode(&req)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := server.Log.Append(req.Record)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	response := ProduceResponse{
		Offset: offset,
	}
	err = json.NewEncoder(writer).Encode(response)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *httpServer) handleConsume(writer http.ResponseWriter, request *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(request.Body).Decode(&req)
	if err == nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := server.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(writer, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	response := ConsumeResponse{
		Record: record,
	}
	err = json.NewEncoder(writer).Encode(response)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func NewHTTPServer(address string) *http.Server {
	httpserv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpserv.handleConsume).Methods("GET")
	r.HandleFunc("/", httpserv.handleProduce).Methods("POST")

	return &http.Server{
		Addr:           address,
		Handler:        r,
	}
}

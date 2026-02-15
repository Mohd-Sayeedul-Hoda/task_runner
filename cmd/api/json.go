package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type envelope map[string]any

func respondWithJson(w http.ResponseWriter, status int, payload any) {

	w.Header().Set("Content-Type", "application/json")

	data, err := json.Marshal(payload)
	if err != nil {
		slog.Error("unable to marshall payload", "err", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(status)
	w.Write(data)

}

func respondWithError(w http.ResponseWriter, status int, msg string, err error) {
	if err != nil {
		slog.Error("error response", "err", err)
	}

	type errorResponse struct {
		Error string `json:"error"`
	}
	respondWithJson(w, status, errorResponse{
		Error: msg,
	})
}

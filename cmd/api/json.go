package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

type envelope map[string]any

var errInvalidRequest = errors.New("invalid request")

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

func decode[T any](w http.ResponseWriter, r *http.Request, data *T) error {

	maxBytes := 1_048_576
	r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytes))
	dec := json.NewDecoder(r.Body)

	dec.DisallowUnknownFields()

	err := dec.Decode(data)
	if err != nil {
		var syntaxError *json.SyntaxError
		var unmarshalTypeError *json.UnmarshalTypeError
		var invalidUnmarshalError *json.InvalidUnmarshalError
		var maxBytesError *http.MaxBytesError

		switch {
		case errors.As(err, &syntaxError):
			return fmt.Errorf("%w: body contain badly-formated JSON (at character %d)", errInvalidRequest, syntaxError.Offset)

		case errors.Is(err, io.ErrUnexpectedEOF):
			return fmt.Errorf("%w: body contains badly-formated JSON", errInvalidRequest)

		case errors.As(err, &unmarshalTypeError):
			if unmarshalTypeError.Field != "" {
				return fmt.Errorf("%w: body contains incorrect JSON type for field %q", errInvalidRequest, unmarshalTypeError.Field)
			}
			return fmt.Errorf("%w: body contians incorrect JSON type (at character %d)", errInvalidRequest, unmarshalTypeError.Offset)

		// happen when body is empty
		case errors.Is(err, io.EOF):
			return fmt.Errorf("%w: body must not be empty", errInvalidRequest)

		case strings.HasPrefix(err.Error(), "json: unknown field"):
			fieldName := strings.TrimPrefix(err.Error(), "json: unknown field")
			return fmt.Errorf("%w: body contains unknown key %s", errInvalidRequest, fieldName)

		// happen when body size bigger then specified size
		case errors.As(err, &maxBytesError):
			return fmt.Errorf("%w: body must not be larger than %d bytes", errInvalidRequest, maxBytesError.Limit)

			// invalid argument when pass to decode function
		case errors.As(err, &invalidUnmarshalError):
			panic(err)

		default:
			return err
		}
	}

	return nil
}

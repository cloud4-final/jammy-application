package codec

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/cloud4-final/jammy-application/libs/go/gate"
	pb "github.com/cloud4-final/jammy-application/libs/go/pb/jammyv1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type HTTPError struct {
	Status int
	Msg    string
}

func (e *HTTPError) Error() string { return e.Msg }

// DecodeBatch enforces request size limits and checks the envelope.
// The returned raw bytes preserve the decompressed wire format.
func DecodeBatch(body []byte, contentType, contentEncoding string, maxBody, maxDecompressed int64) (*pb.EventBatch, []byte, error) {
	if int64(len(body)) > maxBody {
		return nil, nil, &HTTPError{http.StatusRequestEntityTooLarge, "request body exceeds size limit"}
	}
	raw := body
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "zstd":
		var err error
		raw, err = decodeZstd(body, maxDecompressed)
		if err != nil {
			return nil, nil, err
		}
	case "":
		if int64(len(raw)) > maxDecompressed {
			return nil, nil, &HTTPError{http.StatusRequestEntityTooLarge, "decoded body exceeds size limit"}
		}
	default:
		return nil, nil, &HTTPError{http.StatusBadRequest, "unsupported content encoding"}
	}

	batch := new(pb.EventBatch)
	mediaType, _, _ := strings.Cut(contentType, ";")
	var err error
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "application/x-protobuf":
		err = proto.Unmarshal(raw, batch)
	case "application/json":
		err = decodeJSON(raw, batch)
	default:
		return nil, nil, &HTTPError{http.StatusBadRequest, "unsupported content type"}
	}
	if err != nil || !gate.ValidBatch(batch) {
		return nil, nil, &HTTPError{http.StatusBadRequest, "invalid event batch"}
	}
	return batch, raw, nil
}

func decodeJSON(raw []byte, batch *pb.EventBatch) error {
	// ProtoJSON handles duplicate keys and aliases, integer parsing, and
	// base64 padding. Go's base64 decoder silently ignores newlines, so
	// validate the decoded text when JSON escapes could introduce them.
	if bytes.IndexByte(raw, '\\') >= 0 {
		var wire struct {
			Events []struct {
				Payload string `json:"payload"`
			} `json:"events"`
		}
		if err := json.Unmarshal(raw, &wire); err != nil {
			return err
		}
		for _, event := range wire.Events {
			if strings.ContainsAny(event.Payload, "\r\n") {
				return errors.New("invalid base64 payload")
			}
		}
	}
	return protojson.Unmarshal(raw, batch)
}

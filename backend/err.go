package backend

import "errors"

var (
	ErrBadRequest = errors.New("Bad Request")
	ErrNotFound   = errors.New("Not Found")
	ErrInternal   = errors.New("Internal Error")
	ErrUnknown    = errors.New("Unknown Error")
)

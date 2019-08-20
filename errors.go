package badgerquery

import (
	"errors"
	"strings"
)

var (
	ErrDBClosed       = errors.New("db already closed")
	ErrTableExists    = errors.New("table already exists")
	ErrTableNotExists = errors.New("table not exists")

	ErrIndexNotExists = errors.New("index not exists")
)

type MultiError struct {
	errors []error
}

func NewMultiError() *MultiError {
	return &MultiError{
		errors: []error{},
	}
}

func (this *MultiError) HasErrors() bool {
	return len(this.errors) > 0
}

func (this *MultiError) Add(err error) {
	if err != nil {
		this.errors = append(this.errors, err)
	}
}
func (this *MultiError) Error() string {
	errMsgs := []string{}
	for _, err := range this.errors {
		errMsgs = append(errMsgs, err.Error())
	}
	return strings.Join(errMsgs, "\n")
}

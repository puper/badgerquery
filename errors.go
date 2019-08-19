package badgerquery

import "strings"

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

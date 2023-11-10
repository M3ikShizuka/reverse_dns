package errs

import "errors"

var (
	ErrConvertContext           = errors.New("can't convert contextinf")
	ErrConvertInterfaceToStruct = errors.New("cannot convert the record interface to the struct type")
)

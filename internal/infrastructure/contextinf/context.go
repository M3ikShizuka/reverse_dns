package contextinf

import (
	"golang.org/x/net/context"
)

func IsContextCanceled(ctx context.Context) (bool, error) {
	//ctx, ok := ctx.(*gin.Context)
	//if ok == false {
	//	return true, errs.ErrConvertContext
	//}

	select {
	case <-ctx.Done():
		// abort
		return true, nil
	default:
	}

	return false, nil
}

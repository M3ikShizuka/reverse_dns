package v1

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"reversedns/internal/transport/http/response"
)

func (h *HandlerReverseDNSAPI) fqdnPost(ctx *gin.Context) {
	// api/v1/fqdn method POST
	// может приходить до 100к fqdn

	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		response.AbortMessage(ctx, http.StatusBadRequest, err.Error())
		return
	}

	var fqdns []string
	err = json.Unmarshal(body, &fqdns)
	if err != nil {
		response.AbortMessage(ctx, http.StatusBadRequest, err.Error())
		return
	}

	fqdnsSize := len(fqdns)
	if fqdnsSize < 1 {
		msg := "Data has not been transmitted."
		response.AbortMessage(ctx, http.StatusBadRequest, msg)
		ctx.JSON(http.StatusFound, gin.H{
			"message": msg,
		})
		return
	} else if fqdnsSize > 100000 {
		msg := fmt.Sprintf("There's too much data being transmitted. Number of fqdns(%d) > 100000", fqdnsSize)
		response.AbortMessage(ctx, http.StatusBadRequest, msg)
		ctx.JSON(http.StatusFound, gin.H{
			"message": msg,
		})
		return
	}

	// Fix length.
	if len(fqdns) != cap(fqdns) {
		fqdnsNew := make([]string, len(fqdns))
		copy(fqdnsNew, fqdns)
		fqdns = fqdnsNew
	}

	go func() {
		// Process fqdns
		err = h.services.DNSService.ProcessNewlyReceivedDNSData(fqdns)
		if err != nil {
			return
		}
	}()

	// Response
	ctx.Status(http.StatusAccepted)
}

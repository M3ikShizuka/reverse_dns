package v1

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"reversedns/internal/transport/http/response"
)

func (h *HandlerReverseDNSAPI) domainPost(ctx *gin.Context) {
	// api/v1/domain method POST
	// Request:
	// Body: - ["10.1.1.1", "10.1.1.2"]
	// Response:
	// { "10.1.1.1": ["ya.ru", "vk.com", "video.vk.com","ds01.msk.video.vk.com", "dzen.ru"], "10.1.1.2": ["google.ru", "youtube.com", "youtube.com", "news.google.com"] }
	// число ip в одном запросе до 10к

	body, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		response.AbortMessage(ctx, http.StatusBadRequest, err.Error())
		return
	}

	var ips []string
	err = json.Unmarshal(body, &ips)
	if err != nil {
		response.AbortMessage(ctx, http.StatusBadRequest, err.Error())
		return
	}

	ipsSize := len(ips)
	if ipsSize < 1 {
		msg := "Data has not been transmitted."
		response.AbortMessage(ctx, http.StatusBadRequest, msg)
		ctx.JSON(http.StatusFound, gin.H{
			"message": msg,
		})
		return
	} else if ipsSize > 10000 {
		msg := fmt.Sprintf("There's too much data being transmitted. Number of ips(%d) > 10000", ipsSize)
		response.AbortMessage(ctx, http.StatusBadRequest, msg)
		ctx.JSON(http.StatusFound, gin.H{
			"message": msg,
		})
		return
	}

	// Fix length.
	if len(ips) != cap(ips) {
		ipsNew := make([]string, len(ips))
		copy(ipsNew, ips)
		ips = ipsNew
	}

	// Retrieving data from the database.
	ipDomains, err := h.services.DNSService.GetDomainsByIP(ctx, ips)
	if err != nil {
		response.AbortMessage(ctx, http.StatusInternalServerError, "Can't get data from base!")
		return
	}

	// Send success response.
	ctx.IndentedJSON(http.StatusOK, ipDomains)
}

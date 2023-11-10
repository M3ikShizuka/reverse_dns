package v1

import (
	"github.com/gin-gonic/gin"
	"reversedns/internal/service"
)

const (
	// Paths v1
	pathFQDN   string = "/fqdn"
	pathDomain string = "/domain"
)

type HandlerReverseDNSAPI struct {
	services *service.Services
}

func NewHandlerReverseDNSAPI(services *service.Services) *HandlerReverseDNSAPI {
	return &HandlerReverseDNSAPI{
		services: services,
	}
}

func (h *HandlerReverseDNSAPI) Init(router *gin.RouterGroup) {
	v1 := router.Group("/api/v1")
	{
		h.initHandlersReverseDNS(v1)
	}
}

func (h *HandlerReverseDNSAPI) initHandlersReverseDNS(router *gin.RouterGroup) {
	router.POST(pathFQDN, h.fqdnPost)
	router.POST(pathDomain, h.domainPost)
}

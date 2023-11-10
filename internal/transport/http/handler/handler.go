package handler

import (
	"github.com/gin-gonic/gin"
	"reversedns/internal/service"
	"reversedns/internal/transport/http/handler/api/v1"
)

type Handler struct {
	services *service.Services
}

func NewHandler(services *service.Services) *Handler {
	return &Handler{
		services: services,
	}
}

func (h *Handler) Init() *gin.Engine {
	router := gin.Default()
	// Init API
	h.initAPI(router)

	return router
}

func (h *Handler) initAPI(router *gin.Engine) {
	// Init API v1 h.
	handlersV1 := v1.NewHandlerReverseDNSAPI(h.services)
	handlersV1.Init(&router.RouterGroup)
}

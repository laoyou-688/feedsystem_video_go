package canal

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Sync(c *gin.Context) {
	if h == nil || h.service == nil || !h.service.Enabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "canal sync disabled"})
		return
	}
	if !h.service.ValidateToken(c.GetHeader("X-Canal-Token")) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid canal token"})
		return
	}

	var req SyncRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	processed, err := h.service.Process(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, SyncResponse{Processed: processed})
}

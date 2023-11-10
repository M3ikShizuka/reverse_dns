package uuid

import (
	"github.com/google/uuid"
	"reversedns/internal/service/data"
)

func GenerateUUID() data.UUID {
	return data.UUID(uuid.New())
}

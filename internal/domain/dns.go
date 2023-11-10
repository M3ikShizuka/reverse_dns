package domain

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type DNSRecords struct {
	ID   primitive.ObjectID `bson:"_id,omitempty"`
	A    string             `bson:"a"`
	Fqdn string             `bson:"fqdn"`
}

type DNSRecordLifetime struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	DNSRecordID primitive.ObjectID `bson:"dns_id"`
	CreatedAt   time.Time          `bson:"created_at"`
	ExpiredAt   time.Time          `bson:"expired_at"`
}

type DNSNonHistorical struct {
	ID                  primitive.ObjectID `bson:"_id,omitempty"`
	DNSRecordLifetimeID primitive.ObjectID `bson:"dns_record_lifetime_id"`
}

package repository

import (
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/context"
	"reversedns/internal/config"
	"reversedns/internal/domain"
	"reversedns/internal/service/errs"
	"reversedns/internal/service/interfaces"
	"time"
)

type DNSRepositoryMongo struct {
	client   *mongo.Client
	dbConfig *config.Database
}

var _ interfaces.DNSRepository = &DNSRepositoryMongo{}

type RecordTime struct {
	ExpiredAt time.Time `bson:"expired_at"`
}

type DNSFqdn struct {
	Fqdn string `bson:"fqdn"`
}

type DNSRecords struct {
	ID primitive.ObjectID `bson:"_id,omitempty"`
}

type DNSRecordLifetimeID struct {
	ID primitive.ObjectID `bson:"dns_record_lifetime_id"`
}

type DNSNonHistorical struct {
	DNSRecordLifetimeID primitive.ObjectID `bson:"dns_record_lifetime_id"`
}

func NewDNSRepo(client *mongo.Client, dbConfig *config.Database) *DNSRepositoryMongo {
	return &DNSRepositoryMongo{client, dbConfig}
}

func (r *DNSRepositoryMongo) GetDomainsByIP(ctx context.Context, ip string) ([]string, error) {
	db := r.client.Database(r.dbConfig.DBName)
	collDNSRecord := db.Collection(r.dbConfig.Collect.DNSRecord)
	// Retrieving data from the database by IP address
	// and which are historical (active)
	/*
			db.dns_record.aggregate([
		    {
		        $lookup: {
		            from: "dns_record_lifetime",
		            localField: "_id",
		            foreignField: "dns_id",
		            as: "dns_record_lifetime"
		        }
		    },
		    {
		        $unwind: "$dns_record_lifetime"
		    },
		    {
		        $lookup: {
		        from: "dns_non_historical",
		        let: { dns_record_lifetime_id: "$dns_record_lifetime._id" },
		        pipeline: [
		          {
		            $match: {
		              $expr: { $eq: ["$$dns_record_lifetime_id", "$dns_record_lifetime_id"] }
		            }
		          },
		          { $limit: 1 }
		        ],
		        as: "dns_non_historical"
		      }
		    },
		    {
		        $match: {
		            dns_non_historical: [],
		        }
		    },
		    {
		        $unset: "dns_non_historical"
		    },
		    {
		        $match: {
		            "a": IP_ADDRESS_HERE
		        }
		    },
		    {
		        $project: {
		          _id: 0,
		          "a": 1,
		          "fqdn": 1,
		          // "created_at": "$dns_record_lifetime.created_at",
		          // "expired_at": "$dns_record_lifetime.expired_at",
		        }
		    }
		])
	*/

	lookupDNSRecordLifetime := bson.D{
		{"$lookup", bson.D{
			{"from", "dns_record_lifetime"},
			{"localField", "_id"},
			{"foreignField", "dns_id"},
			{"as", "dns_record_lifetime"},
		}},
	}
	unwind := bson.D{
		{"$unwind", "$dns_record_lifetime"},
	}
	lookupDNSNonHistorical := bson.D{
		{"$lookup", bson.D{
			{"from", "dns_non_historical"},
			{"let", bson.D{
				{"dns_record_lifetime_id", "$dns_record_lifetime._id"},
			}},
			{"pipeline", bson.A{
				bson.D{
					{"$match", bson.D{
						{"$expr", bson.D{
							{"$eq", bson.A{
								"$$dns_record_lifetime_id",
								"$dns_record_lifetime_id",
							}},
						}},
					}},
				},
				bson.D{
					{"$limit", 1},
				},
			}},
			{"as", "dns_non_historical"},
		}},
	}
	match := bson.D{
		{"$match", bson.D{
			{"dns_non_historical", bson.A{}},
		}},
	}
	unset := bson.D{
		{"$unset", "dns_non_historical"},
	}
	match2 := bson.D{
		{"$match", bson.D{
			{"a", ip},
		}},
	}
	project := bson.D{
		{"$project", bson.D{
			{"_id", 0},
			{"a", 1},
			{"fqdn", 1},
		}},
	}

	cursor, err := collDNSRecord.Aggregate(ctx, mongo.Pipeline{
		lookupDNSRecordLifetime,
		unwind,
		lookupDNSNonHistorical,
		match,
		unset,
		match2,
		project,
	})
	if err != nil {
		return nil, err
	}

	domains := make([]string, 0, cursor.RemainingBatchLength())

	for cursor.Next(ctx) {
		var dnsFqdn DNSFqdn
		err = cursor.Decode(&dnsFqdn)
		if err != nil {
			return nil, err
		}

		var domainName string
		if len(dnsFqdn.Fqdn) > 0 {
			domainName = dnsFqdn.Fqdn[:len(dnsFqdn.Fqdn)-1]
		}

		domains = append(domains, domainName)
	}

	// Fix the memory size for data storage.
	var domainsResult []string
	if len(domains) != cap(domains) {
		domainsResult = make([]string, len(domains))
		copy(domainsResult, domains)
	} else {
		domainsResult = domains
	}

	return domainsResult, nil
}

func (r *DNSRepositoryMongo) GetDataRequiringUpdate(ctx context.Context, operationStartTimePtr *time.Time, lastUpdateTimePtr *time.Time) ([]string, error) {
	// Retrieving data from the database for which TTL has expired
	// and which are historical (active)
	// Obtaining unique FQDNSs that need to be updated.
	/*
	  db.dns_record_lifetime.aggregate([
	    {
	        $lookup: {
	            from: "dns_non_historical",
	            localField: "_id",
	            foreignField: "dns_record_lifetime_id",
	            as: "dns_non_historical"
	        }
	    },
	    {
	        $lookup: {
	            from: "dns_record",
	            localField: "dns_id",
	            foreignField: "_id",
	            as: "dns_record"
	        }
	    },
	    {
	        $unwind: "$dns_record"
	    },
	    {
	        $match: {
	            dns_non_historical: [],
	            expired_at: { $lte: *operationStartTimePtr, $gt: *lastUpdateTimePtr }
	        }
	    },
	    {
	        $unset: "dns_non_historical"
	    },
	    { $group: {_id: null, fqdn: {$addToSet: "$dns_record.fqdn"}}},
	    { $unwind: "$fqdn" },
	    {
	        $project: {
	            _id: 0,
	            fqdn: "$fqdn"
	        }
	    }
	  ])
	*/

	lookupDNSNonHistorical := bson.D{
		{"$lookup", bson.D{
			{"from", "dns_non_historical"},
			{"localField", "_id"},
			{"foreignField", "dns_record_lifetime_id"},
			{"as", "dns_non_historical"},
		}},
	}
	lookupDNSRecord := bson.D{
		{"$lookup", bson.D{
			{"from", "dns_record"},
			{"localField", "dns_id"},
			{"foreignField", "_id"},
			{"as", "dns_record"},
		}},
	}
	unwindDNSRecord := bson.D{
		{"$unwind", "$dns_record"},
	}
	match := bson.D{
		{"$match", bson.D{
			{"dns_non_historical", bson.A{}},
			{"expired_at", bson.D{
				{"$lte", *operationStartTimePtr},
				{"$gt", *lastUpdateTimePtr},
			}},
		}},
	}
	unset := bson.D{
		{"$unset", "dns_non_historical"},
	}
	group := bson.D{
		{"$group", bson.D{
			{"_id", nil},
			{"fqdn", bson.D{
				{"$addToSet", "$dns_record.fqdn"},
			}},
		}},
	}
	unwindFqdn := bson.D{
		{"$unwind", "$fqdn"},
	}
	project := bson.D{
		{"$project", bson.D{
			{"_id", 0},
			{"fqdn", "$fqdn"},
		}},
	}

	db := r.client.Database(r.dbConfig.DBName)
	collDNSRecordLifetime := db.Collection(r.dbConfig.Collect.DNSRecordLifetime)
	cursor, err := collDNSRecordLifetime.Aggregate(ctx, mongo.Pipeline{
		lookupDNSNonHistorical,
		lookupDNSRecord,
		unwindDNSRecord,
		match,
		unset,
		group,
		unwindFqdn,
		project,
	})
	if err != nil {
		return nil, err
	}

	fqdns := make([]string, 0, cursor.RemainingBatchLength())

	for cursor.Next(ctx) {
		var dnsFqdn DNSFqdn
		err = cursor.Decode(&dnsFqdn)
		if err != nil {
			return nil, err
		}

		fqdns = append(fqdns, dnsFqdn.Fqdn)
	}

	// Fix the memory size for data storage.
	var fqdnsResult []string
	if len(fqdns) != cap(fqdns) {
		fqdnsResult = make([]string, len(fqdns))
		copy(fqdnsResult, fqdns)
	} else {
		fqdnsResult = fqdns
	}

	return fqdnsResult, nil
}

func (r *DNSRepositoryMongo) UpdateDNS(ctx context.Context, records []interface{}, recordLifetime []interface{}, operationStartTimePtr *time.Time) error {
	ctxs, ok := ctx.(mongo.SessionContext)
	if !ok {
		return errs.ErrConvertContext
	}

	db := r.client.Database(r.dbConfig.DBName)
	collDNSRecord := db.Collection(r.dbConfig.Collect.DNSRecord)
	collDNSRecordLifetime := db.Collection(r.dbConfig.Collect.DNSRecordLifetime)
	collDNSNonHistorical := db.Collection(r.dbConfig.Collect.DNSNonHistorical)

	// Is collection empty
	var isDNSRecordsEmpty bool
	isDNSRecordsEmpty, err := r.IsCollectionEmpty(ctx)
	if err != nil {
		ctxs.AbortTransaction(ctx)
		return err
	}

	var dnsNonHistoricals []interface{}
	if !isDNSRecordsEmpty {
		// Get fqdn.
		record, ok := records[0].(domain.DNSRecords)
		if !ok {
			ctxs.AbortTransaction(ctx)
			return errs.ErrConvertInterfaceToStruct
		}

		fqdn := record.Fqdn

		// Search all historical records by expired_at and FQDN
		// if the collection is not empty.
		// Get dns_record_lifetime_id to be added to dns_non_historical.
		// Later, mark them as non-historical.
		/*
			db.dns_record.aggregate([
			    {
			        $lookup: {
			            from: "dns_record_lifetime",
			            localField: "_id",
			            foreignField: "dns_id",
			            as: "dns_record_lifetime"
			        }
			    },
			    {
			        $unwind: "$dns_record_lifetime"
			    },
			    {
			        $lookup: {
			        from: "dns_non_historical",
			        let: { dns_record_lifetime_id: "$dns_record_lifetime._id" },
			        pipeline: [
			          {
			            $match: {
			              $expr: { $eq: ["$$dns_record_lifetime_id", "$dns_record_lifetime_id"] }
			            }
			          },
			          { $limit: 1 }
			        ],
			        as: "dns_non_historical"
			      }
			    },
			    {
			        $match: {
			            dns_non_historical: [],
			            "fqdn": DOMAIN,
			            "dns_record_lifetime.created_at": { $lt: *operationStartTimePtr } // date
			        }
			    },
			    {
			        $unset: "dns_non_historical"
			    },
			    {
			        $project: {
			            _id: 0,
			            dns_record_lifetime_id: "$dns_record_lifetime._id",
			            a: 1,
			            fqdn: 1
			        }
			    }
			])
		*/

		lookupDNSRecordLifetime := bson.D{
			{"$lookup", bson.D{
				{"from", "dns_record_lifetime"},
				{"localField", "_id"},
				{"foreignField", "dns_id"},
				{"as", "dns_record_lifetime"},
			}},
		}
		unwind := bson.D{
			{"$unwind", "$dns_record_lifetime"},
		}
		lookupDNSNonHistorical := bson.D{
			{"$lookup", bson.D{
				{"from", "dns_non_historical"},
				{"let", bson.D{
					{"dns_record_lifetime_id", "$dns_record_lifetime._id"},
				}},
				{"pipeline", bson.A{
					bson.D{
						{"$match", bson.D{
							{"$expr", bson.D{
								{"$eq", bson.A{
									"$$dns_record_lifetime_id",
									"$dns_record_lifetime_id",
								}},
							}},
						}},
					},
					bson.D{
						{"$limit", 1},
					},
				}},
				{"as", "dns_non_historical"},
			}},
		}
		match := bson.D{
			{"$match", bson.D{
				{"dns_non_historical", bson.A{}},
				{"fqdn", fqdn},
				{"dns_record_lifetime.created_at", bson.D{
					{"$lt", *operationStartTimePtr},
				}},
			}},
		}
		unset := bson.D{
			{"$unset", "dns_non_historical"},
		}
		project := bson.D{
			{"$project", bson.D{
				{"_id", 0},
				{"dns_record_lifetime_id", "$dns_record_lifetime._id"},
				{"a", 1},
				{"fqdn", 1},
			}},
		}

		cursor, err := collDNSRecord.Aggregate(ctx, mongo.Pipeline{
			lookupDNSRecordLifetime,
			unwind,
			lookupDNSNonHistorical,
			match,
			unset,
			project,
		})
		if err != nil {
			ctxs.AbortTransaction(ctx)
			return err
		}

		// Later, mark them as non-historical.
		dnsNonHistoricals = make([]interface{}, 0, cursor.RemainingBatchLength())

		for cursor.Next(ctx) {
			var dnsRecordLifetimeID DNSRecordLifetimeID
			err = cursor.Decode(&dnsRecordLifetimeID)
			if err != nil {
				ctxs.AbortTransaction(ctx)
				return err
			}

			dnsNonHistoricals = append(dnsNonHistoricals, DNSNonHistorical{dnsRecordLifetimeID.ID})
		}
	}

	// Insert new DSN records if it is not in the database.
	for index, recordI := range records {
		record, ok := recordI.(domain.DNSRecords)
		if !ok {
			ctxs.AbortTransaction(ctx)
			return errs.ErrConvertInterfaceToStruct
		}

		/*
				// Try to find dns_record in database.
				db.dns_record.find(
			  {
			    "a": "173.194.222.103",
			    "fqdn": "google.com."
			  },
			  {
			    "_id": 1
			  })
		*/

		filter := bson.D{
			{"a", record.A},
			{"fqdn", record.Fqdn},
		}
		opts := options.FindOne().SetProjection(bson.M{
			"_id": 1,
		})

		var dnsRecordID primitive.ObjectID
		var dnsRecords DNSRecords
		err := collDNSRecord.FindOne(ctx, filter, opts).Decode(&dnsRecords)
		if err == nil {
			dnsRecordID = dnsRecords.ID
			if err != nil {
				ctxs.AbortTransaction(ctx)
				return err
			}
		} else {
			if !errors.Is(err, mongo.ErrNoDocuments) {
				ctxs.AbortTransaction(ctx)
				return err
			}

			// Your query did not match any documents.
			// Insert new dns_records if they are not in the database.
			doc := bson.D{
				{"a", record.A},
				{"fqdn", record.Fqdn},
			}
			result, err := collDNSRecord.InsertOne(ctx, doc)
			if err != nil {
				ctxs.AbortTransaction(ctx)
				return err
			}

			dnsRecordID, ok = result.InsertedID.(primitive.ObjectID)
			if !ok {
				ctxs.AbortTransaction(ctx)
				return errs.ErrConvertInterfaceToStruct
			}
		}

		// Insert dnsRecordId into the corresponding DNSRecordLifetime
		rlt, ok := recordLifetime[index].(domain.DNSRecordLifetime)
		if !ok {
			ctxs.AbortTransaction(ctx)
			return errs.ErrConvertInterfaceToStruct
		}

		rlt.DNSRecordID = dnsRecordID
		recordLifetime[index] = rlt
	}

	// Insert new DSN record lifetime.
	_, err = collDNSRecordLifetime.InsertMany(ctx, recordLifetime)
	if err != nil {
		ctxs.AbortTransaction(ctx)
		return err
	}

	// Mark old records_lifetime as non-historical.
	if len(dnsNonHistoricals) > 0 {
		for {
			_, err := collDNSNonHistorical.InsertMany(ctx, dnsNonHistoricals)
			if err != nil {
				// If transient error, retry operation.
				if cmdErr, ok := err.(mongo.CommandError); ok && cmdErr.HasErrorLabel("TransientTransactionError") {
					continue
				} else {
					ctxs.AbortTransaction(ctx)
					return err
				}
			}
			break
		}
	}

	if err = ctxs.CommitTransaction(ctx); err != nil {
		ctxs.AbortTransaction(ctx)
		return err
	}

	return nil
}

func (r *DNSRepositoryMongo) DoTransaction(ctx context.Context, handler func(sctx context.Context) error) error {
	// Start transaction
	session, err := r.client.StartSession()
	if err != nil {
		session.EndSession(ctx)
		return err
	}

	err = session.StartTransaction()
	if err != nil {
		session.EndSession(ctx)
		return err
	}

	handlerIn := func(sctx mongo.SessionContext) error {
		return handler(sctx)
	}

	if err = mongo.WithSession(ctx, session, handlerIn); err != nil {
		session.EndSession(ctx)
		return err
	}
	session.EndSession(ctx)

	return nil
}

func (r *DNSRepositoryMongo) IsCollectionEmpty(ctx context.Context) (bool, error) {
	coll := r.client.Database(r.dbConfig.DBName).Collection(r.dbConfig.Collect.DNSRecord)

	// Is collection empty
	var isEmpty bool
	var dnsRecords DNSRecords
	opts := options.FindOne().SetProjection(bson.M{
		"_id": 1,
	})

	err := coll.FindOne(ctx, bson.D{}, opts).Decode(&dnsRecords)
	if err == nil {
		isEmpty = false
	} else {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return true, err
		}

		isEmpty = true
	}

	return isEmpty, nil
}

func (r *DNSRepositoryMongo) InitDataBaseStructure(ctx context.Context) error {
	db := r.client.Database(r.dbConfig.DBName)

	// Create collections
	err := db.CreateCollection(ctx, r.dbConfig.Collect.DNSRecord)
	if err != nil {
		return err
	}

	err = db.CreateCollection(ctx, r.dbConfig.Collect.DNSRecordLifetime)
	if err != nil {
		return err
	}

	err = db.CreateCollection(ctx, r.dbConfig.Collect.DNSNonHistorical)
	if err != nil {
		return err
	}

	// Create indexes indexes
	collDnsRecord := db.Collection(r.dbConfig.Collect.DNSRecord)

	// db.dns_record.createIndex( { "a": 1 } )
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{"a", 1},
		},
	}
	_, err = collDnsRecord.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	// db.dns_record.createIndex( { "fqdn": 1 } )
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{"fqdn", 1},
		},
	}
	_, err = collDnsRecord.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	// db.dns_record.createIndex( { "a": 1, "fqdn": 1 }, { unique: true } )
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{"a", 1},
			{"fqdn", 1},
		},
		Options: options.Index().SetUnique(true),
	}
	_, err = collDnsRecord.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	// db.dns_record_lifetime.createIndex( { "expired_at": 1 } )
	collDnsRecordLifetime := db.Collection(r.dbConfig.Collect.DNSRecordLifetime)
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{"expired_at", 1},
		},
	}
	_, err = collDnsRecordLifetime.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	// db.dns_record_lifetime.createIndex( { "dns_id": 1 } )
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{"dns_id", 1},
		},
	}
	_, err = collDnsRecordLifetime.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	// db.dns_non_historical.createIndex( { "dns_record_lifetime_id": 1 }, { unique: true } )
	collDnsNonHistorical := db.Collection(r.dbConfig.Collect.DNSNonHistorical)
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{"dns_record_lifetime_id", 1},
		},
		Options: options.Index().SetUnique(true),
	}
	_, err = collDnsNonHistorical.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	return nil
}

func (r *DNSRepositoryMongo) GetNearTimeOfObsoleteRecord(ctx context.Context) (*time.Time, error) {
	db := r.client.Database(r.dbConfig.DBName)
	collDnsRecordLifetime := db.Collection(r.dbConfig.Collect.DNSRecordLifetime)

	// Take the lowest expired_at value, from historical records.
	lookupDnsNonHistorical := bson.D{
		{"$lookup", bson.D{
			{"from", "dns_non_historical"},
			{"localField", "_id"},
			{"foreignField", "dns_record_lifetime_id"},
			{"as", "dns_non_historical"},
		}},
	}
	match := bson.D{
		{"$match", bson.D{
			{"dns_non_historical", bson.A{}},
		}},
	}
	unset := bson.D{
		{"$unset", "dns_non_historical"},
	}
	sort := bson.D{
		{"$sort", bson.D{
			{"expired_at", 1},
		}},
	}
	limit := bson.D{
		{"$limit", 1},
	}
	project := bson.D{
		{"$project", bson.D{
			{"_id", 0},
			{"expired_at", 1},
		}},
	}

	cursor, err := collDnsRecordLifetime.Aggregate(ctx, mongo.Pipeline{
		lookupDnsNonHistorical,
		match,
		unset,
		sort,
		limit,
		project,
	})
	if err != nil {
		return nil, err
	}

	var recTime RecordTime
	if cursor.Next(ctx) {
		err = cursor.Decode(&recTime)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	return &recTime.ExpiredAt, nil
}

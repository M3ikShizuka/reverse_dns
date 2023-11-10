records:
reverse_dns.dns_recrods: 1774
reverse_dns.dns_record_lifetime: 1792

(pprof) list UpdateDNS           
Total: 2.41s
ROUTINE ======================== reversedns/internal/infrastructure/repository.(*DNSRepositoryMongo).UpdateDNS in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/infrastructure/repository/dns_mongo.go
         0      1.25s (flat, cum) 51.87% of Total
         .          .    339:func (r *DNSRepositoryMongo) UpdateDNS(ctx context.Context, records []interface{}, recordLifetime []interface{}, operationStartTimePtr *time.Time) error {
         .          .    340:   ctxs, ok := ctx.(mongo.SessionContext)
         .          .    341:   if ok == false {
         .          .    342:           return errs.ErrConvertContext
         .          .    343:   }
         .          .    344:
         .          .    345:   db := r.client.Database(r.dbConfig.DBName)
         .          .    346:   collDnsRecord := db.Collection(r.dbConfig.Collect.DnsRecord)
         .          .    347:   collDnsRecordLifetime := db.Collection(r.dbConfig.Collect.DnsRecordLifetime)
         .          .    348:   collDnsNonHistorical := db.Collection(r.dbConfig.Collect.DnsNonHistorical)
         .          .    349:
         .          .    350:   // Find all historical records by date and FQDN
         .      270ms    351:   count, err := collDnsRecord.CountDocuments(ctx, bson.M{})
         .          .    352:   if err != nil {
         .          .    353:           _ = ctxs.AbortTransaction(ctx)
         .          .    354:           return err
         .          .    355:   }
         .          .    356:
         .          .    357:   var dnsNonHistoricals []interface{}
         .          .    358:   if count > 0 {
         .          .    359:           // Get fqdn.
         .          .    360:           record, ok := records[0].(domain.DNSRecords)
         .          .    361:           if ok == false {
         .          .    362:                   ctxs.AbortTransaction(ctx)
         .          .    363:                   return errs.ErrConvertInterfaceToStruct
         .          .    364:           }
         .          .    365:
         .          .    366:           fqdn := record.Fqdn
         .          .    367:
         .          .    368:           // Search all historical records by expired_at and FQDN
         .          .    369:           // if the collection is not empty.
         .          .    370:           // Get dns_record_lifetime_id to be added to dns_non_historical.
         .          .    371:           // Later, mark them as non-historical.
         .          .    372:           /*
         .          .    373:                   db.dns_record.aggregate([
         .          .    374:                       {
         .          .    375:                           $lookup: {
         .          .    376:                               from: "dns_record_lifetime",
         .          .    377:                               localField: "_id",
         .          .    378:                               foreignField: "dns_id",
         .          .    379:                               as: "dns_record_lifetime"
         .          .    380:                           }
         .          .    381:                       },
         .          .    382:                       {
         .          .    383:                           $unwind: "$dns_record_lifetime"
         .          .    384:                       },
         .          .    385:                       {
         .          .    386:                           $lookup: {
         .          .    387:                           from: "dns_non_historical",
         .          .    388:                           let: { dns_record_lifetime_id: "$dns_record_lifetime._id" },
         .          .    389:                           pipeline: [
         .          .    390:                             {
         .          .    391:                               $match: {
         .          .    392:                                 $expr: { $eq: ["$$dns_record_lifetime_id", "$dns_record_lifetime_id"] }
         .          .    393:                               }
         .          .    394:                             },
         .          .    395:                             { $limit: 1 }
         .          .    396:                           ],
         .          .    397:                           as: "dns_non_historical"
         .          .    398:                         }
         .          .    399:                       },
         .          .    400:                       {
         .          .    401:                           $match: {
         .          .    402:                               dns_non_historical: [],
         .          .    403:                               "fqdn": DOMAIN,
         .          .    404:                               "dns_record_lifetime.created_at": { $lt: *operationStartTimePtr } // date
         .          .    405:                           }
         .          .    406:                       },
         .          .    407:                       {
         .          .    408:                           $unset: "dns_non_historical"
         .          .    409:                       },
         .          .    410:                       {
         .          .    411:                           $project: {
         .          .    412:                               _id: 0,
         .          .    413:                               dns_record_lifetime_id: "$dns_record_lifetime._id",
         .          .    414:                               a: 1,
         .          .    415:                               fqdn: 1
         .          .    416:                           }
         .          .    417:                       }
         .          .    418:                   ])
         .          .    419:           */
         .          .    420:
         .          .    421:           lookupDnsRecordLifetime := bson.D{
         .          .    422:                   {"$lookup", bson.D{
         .          .    423:                           {"from", "dns_record_lifetime"},
         .          .    424:                           {"localField", "_id"},
         .          .    425:                           {"foreignField", "dns_id"},
         .          .    426:                           {"as", "dns_record_lifetime"},
         .          .    427:                   }},
         .          .    428:           }
         .          .    429:           unwind := bson.D{
         .          .    430:                   {"$unwind", "$dns_record_lifetime"},
         .          .    431:           }
         .          .    432:           lookupDnsNonHistorical := bson.D{
         .          .    433:                   {"$lookup", bson.D{
         .          .    434:                           {"from", "dns_non_historical"},
         .          .    435:                           {"let", bson.D{
         .          .    436:                                   {"dns_record_lifetime_id", "$dns_record_lifetime._id"},
         .          .    437:                           }},
         .          .    438:                           {"pipeline", bson.A{
         .          .    439:                                   bson.D{
         .          .    440:                                           {"$match", bson.D{
         .          .    441:                                                   {"$expr", bson.D{
         .          .    442:                                                           {"$eq", bson.A{
         .          .    443:                                                                   "$$dns_record_lifetime_id",
         .          .    444:                                                                   "$dns_record_lifetime_id",
         .          .    445:                                                           }},
         .          .    446:                                                   }},
         .          .    447:                                           }},
         .          .    448:                                   },
         .          .    449:                                   bson.D{
         .          .    450:                                           {"$limit", 1},
         .          .    451:                                   },
         .          .    452:                           }},
         .          .    453:                           {"as", "dns_non_historical"},
         .          .    454:                   }},
         .          .    455:           }
         .          .    456:           match := bson.D{
         .          .    457:                   {"$match", bson.D{
         .          .    458:                           {"dns_non_historical", bson.A{}},
         .          .    459:                           {"fqdn", fqdn},
         .          .    460:                           {"dns_record_lifetime.created_at", bson.D{
         .          .    461:                                   {"$lt", *operationStartTimePtr},
         .          .    462:                           }},
         .          .    463:                   }},
         .          .    464:           }
         .          .    465:           unset := bson.D{
         .          .    466:                   {"$unset", "dns_non_historical"},
         .          .    467:           }
         .          .    468:           project := bson.D{
         .          .    469:                   {"$project", bson.D{
         .          .    470:                           {"_id", 0},
         .          .    471:                           {"dns_record_lifetime_id", "$dns_record_lifetime._id"},
         .          .    472:                           {"a", 1},
         .          .    473:                           {"fqdn", 1},
         .          .    474:                   }},
         .          .    475:           }
         .          .    476:
         .      250ms    477:           cursor, err := collDnsRecord.Aggregate(ctx, mongo.Pipeline{
         .          .    478:                   lookupDnsRecordLifetime,
         .          .    479:                   unwind,
         .          .    480:                   lookupDnsNonHistorical,
         .          .    481:                   match,
         .          .    482:                   unset,
         .          .    483:                   project,
         .          .    484:           })
         .          .    485:           if err != nil {
         .          .    486:                   ctxs.AbortTransaction(ctx)
         .          .    487:                   return err
         .          .    488:           }
         .          .    489:
         .          .    490:           // Later, mark them as non-historical.
         .          .    491:           dnsNonHistoricals = make([]interface{}, 0, cursor.RemainingBatchLength())
         .          .    492:
         .          .    493:           for cursor.Next(ctx) {
         .          .    494:                   var dnsRecordLifetimeId DNSRecordLifetimeID
         .          .    495:                   err = cursor.Decode(&dnsRecordLifetimeId)
         .          .    496:                   if err != nil {
         .          .    497:                           ctxs.AbortTransaction(ctx)
         .          .    498:                           return err
         .          .    499:                   }
         .          .    500:
         .          .    501:                   dnsNonHistoricals = append(dnsNonHistoricals, DNSNonHistorical{dnsRecordLifetimeId.ID})
         .          .    502:           }
         .          .    503:   }
         .          .    504:
         .          .    505:   // Insert new DSN records if it is not in the database.
         .          .    506:   for index, recordI := range records {
         .          .    507:           record, ok := recordI.(domain.DNSRecords)
         .          .    508:           if ok == false {
         .          .    509:                   ctxs.AbortTransaction(ctx)
         .          .    510:                   return errs.ErrConvertInterfaceToStruct
         .          .    511:           }
         .          .    512:
         .          .    513:           /*
         .          .    514:                           // Try to find dns_record in database.
         .          .    515:                           db.dns_record.find(
         .          .    516:                     {
         .          .    517:                       "a": "173.194.222.103",
         .          .    518:                       "fqdn": "google.com."
         .          .    519:                     },
         .          .    520:                     {
         .          .    521:                       "_id": 1
         .          .    522:                     })
         .          .    523:           */
         .          .    524:
         .          .    525:           filter := bson.D{
         .          .    526:                   {"a", record.A},
         .          .    527:                   {"fqdn", record.Fqdn},
         .          .    528:           }
         .          .    529:           opts := options.FindOne().SetProjection(bson.M{
         .          .    530:                   "_id": 1,
         .          .    531:           })
         .          .    532:
         .          .    533:           var dnsRecordId primitive.ObjectID
         .          .    534:           var dnsRecords DNSRecords
         .      280ms    535:           err := collDnsRecord.FindOne(ctx, filter, opts).Decode(&dnsRecords)
         .          .    536:           if err == nil {
         .          .    537:                   dnsRecordId = dnsRecords.ID
         .          .    538:                   if err != nil {
         .          .    539:                           ctxs.AbortTransaction(ctx)
         .          .    540:                           return err
         .          .    541:                   }
         .          .    542:           } else {
         .          .    543:                   if errors.Is(err, mongo.ErrNoDocuments) == false {
         .          .    544:                           ctxs.AbortTransaction(ctx)
         .          .    545:                           return err
         .          .    546:                   }
         .          .    547:
         .          .    548:                   // Your query did not match any documents.
         .          .    549:                   // Insert new dns_records if they are not in the database.
         .          .    550:                   doc := bson.D{
         .          .    551:                           {"a", record.A},
         .          .    552:                           {"fqdn", record.Fqdn},
         .          .    553:                   }
         .      230ms    554:                   result, err := collDnsRecord.InsertOne(ctx, doc)
         .          .    555:                   if err != nil {
         .          .    556:                           ctxs.AbortTransaction(ctx)
         .          .    557:                           return err
         .          .    558:                   }
         .          .    559:
         .          .    560:                   dnsRecordId, ok = result.InsertedID.(primitive.ObjectID)
         .          .    561:                   if ok == false {
         .          .    562:                           ctxs.AbortTransaction(ctx)
         .          .    563:                           return errs.ErrConvertInterfaceToStruct
         .          .    564:                   }
         .          .    565:           }
         .          .    566:
         .          .    567:           // Insert dnsRecordId into the corresponding DNSRecordLifetime
         .          .    568:           rlt, ok := recordLifetime[index].(domain.DNSRecordLifetime)
         .          .    569:           if ok == false {
         .          .    570:                   ctxs.AbortTransaction(ctx)
         .          .    571:                   return errs.ErrConvertInterfaceToStruct
         .          .    572:           }
         .          .    573:
         .          .    574:           rlt.DNSRecordID = dnsRecordId
         .       10ms    575:           recordLifetime[index] = rlt
         .          .    576:   }
         .          .    577:
         .          .    578:   // Insert new DSN record lifetime.
         .      110ms    579:   _, err = collDnsRecordLifetime.InsertMany(ctx, recordLifetime)
         .          .    580:   if err != nil {
         .          .    581:           ctxs.AbortTransaction(ctx)
         .          .    582:           return err
         .          .    583:   }
         .          .    584:
         .          .    585:   // Mark old records_lifetime as non-historical.
         .          .    586:   if len(dnsNonHistoricals) > 0 {
         .          .    587:           _, err := collDnsNonHistorical.InsertMany(ctx, dnsNonHistoricals)
         .          .    588:           if err != nil {
         .          .    589:                   ctxs.AbortTransaction(ctx)
         .          .    590:                   return err
         .          .    591:           }
         .          .    592:   }
         .          .    593:
         .      100ms    594:   if err = ctxs.CommitTransaction(ctx); err != nil {
         .          .    595:           ctxs.AbortTransaction(ctx)
         .          .    596:           return err
         .          .    597:   }
         .          .    598:
         .          .    599:   return nil
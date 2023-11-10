(pprof) list updateDNSInformation
Total: 2.21s
ROUTINE ======================== reversedns/internal/service.(*DNSService).updateDNSInformation.func2 in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/service/dns.go
         0      210ms (flat, cum)  9.50% of Total
         .          .    149:           func(gorIndex int, fqdnsChunk []string) {
         .          .    150:                   defer func() {
         .          .    151:                           if gorCompleted.Load() < int32(gorCountDNS-1) {
         .          .    152:                                   gorCompleted.Add(1)
         .          .    153:                           } else {
         .          .    154:                                   // All the goroutines have finished their work.
         .          .    155:                                   close(cOut)
         .          .    156:                           }
         .          .    157:                   }()
         .          .    158:
         .          .    159:                   for _, fqdn := range fqdnsChunk {
         .          .    160:                           isCanceled, err := isContextCanceled(ctx)
         .          .    161:                           if err != nil {
         .          .    162:                                   processGorErrors(err)
         .          .    163:                                   return
         .          .    164:                           }
         .          .    165:
         .          .    166:                           if isCanceled {
         .          .    167:                                   strOut := fmt.Sprintf("updateDNSInformation(): Context canceled")
         .          .    168:                                   logger.Info(strOut)
         .          .    169:                                   return
         .          .    170:                           }
         .          .    171:
         .          .    172:                           // Obtain DNS information for fqdns from the specified DNS server.
         .      200ms    173:                           dnsInfo, err := d.dns.GetDNSInfo(d.config.DNSClient.DSN, fqdn)
         .          .    174:                           if err != nil {
         .          .    175:                                   switch {
         .          .    176:                                   case os.IsTimeout(err),
         .          .    177:                                           errors.Is(err, os.ErrDeadlineExceeded):
         .          .    178:                                           cOut <- nil
         .          .    179:                                           continue
         .          .    180:                                   }
         .          .    181:
         .          .    182:                                   processGorErrors(err)
         .          .    183:                                   return
         .          .    184:                           }
         .          .    185:
         .          .    186:                           // Nothing to update
         .          .    187:                           if len(dnsInfo) < 1 {
         .       10ms    188:                                   logger.Warn("DNS server did not provide records for fqdn: " + fqdn)
         .          .    189:                                   cOut <- nil
         .          .    190:                                   continue
         .          .    191:                           }
         .          .    192:
         .          .    193:                           // Send data chunk
ROUTINE ======================== reversedns/internal/service.(*DNSService).updateDNSInformation.func3 in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/service/dns.go
         0      1.10s (flat, cum) 49.77% of Total
         .          .    210:           func(gorIndex int, fqdnsChunk []string) {
         .          .    211:                   defer wg.Done()
         .          .    212:           loop:
         .          .    213:                   for {
         .          .    214:                           select {
         .          .    215:                           case dnsInfo, ok := <-cOut:
         .          .    216:                                   if !ok {
         .          .    217:                                           break loop
         .          .    218:                                   }
         .          .    219:
         .          .    220:                                   if dnsInfo == nil {
         .          .    221:                                           continue
         .          .    222:                                   }
         .          .    223:
         .          .    224:                                   // Convert DNSInfo data to model data format.
         .          .    225:                                   dnsRecords, dnsRecordLifetime := d.convertDNSInfoToModel(dnsInfo)
         .          .    226:
         .          .    227:                                   //// TODO debug:
         .          .    228:                                   //func() {
         .          .    229:                                   //      strOut := fmt.Sprintf("updateDNSInformation()\n dnsRecords = GetDNSInfo() count: %d\n", len(dnsRecords))
         .          .    230:                                   //      //for i, dnsRecord := range dnsRecords {
         .          .    231:                                   //      //      strOut = fmt.Sprintf("%sindex: %d\t %v\n", strOut, i, dnsRecord)
         .          .    232:                                   //      //}
         .          .    233:                                   //      logger.Debug(strOut)
         .          .    234:                                   //}()
         .          .    235:
         .          .    236:                                   // Update DNS info in base.
         .      1.10s    237:                                   err := d.DNSRepo.DoTransaction(ctx, func(sctx context.Context) error {
         .          .    238:                                           return d.DNSRepo.UpdateDNS(sctx, dnsRecords, dnsRecordLifetime, operationStartTimePtr)
         .          .    239:                                   })
         .          .    240:                                   if err != nil {
         .          .    241:                                           strOut := fmt.Sprintf("updateDNSInformation()\n FAILED UpdateDNS count: %d dnsRecords: %v dnsRecordLifetime: %v\nerr: %s\n", len(dnsRecords), dnsRecords, dnsRecordLifetime, err)
         .          .    242:                                           logger.Error(strOut)
ROUTINE ======================== reversedns/internal/service.(*DNSService).updateDNSInformation.func3.1 in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/service/dns.go
         0      1.10s (flat, cum) 49.77% of Total
         .          .    237:                                   err := d.DNSRepo.DoTransaction(ctx, func(sctx context.Context) error {
         .      1.10s    238:                                           return d.DNSRepo.UpdateDNS(sctx, dnsRecords, dnsRecordLifetime, operationStartTimePtr)
         .          .    239:                                   })
         .          .    240:                                   if err != nil {
         .          .    241:                                           strOut := fmt.Sprintf("updateDNSInformation()\n FAILED UpdateDNS count: %d dnsRecords: %v dnsRecordLifetime: %v\nerr: %s\n", len(dnsRecords), dnsRecords, dnsRecordLifetime, err)
         .          .    242:                                           logger.Error(strOut)
         .          .    243:
(pprof) list UpdateDNS 
Total: 2.21s
ROUTINE ======================== reversedns/internal/infrastructure/repository.(*DNSRepositoryMongo).UpdateDNS in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/infrastructure/repository/dns_mongo.go
         0      1.10s (flat, cum) 49.77% of Total
         .          .    339:func (r *DNSRepositoryMongo) UpdateDNS(ctx context.Context, records []interface{}, recordLifetime []interface{}, operationStartTimePtr *time.Time) error {
         .          .    340:   ctxs, ok := ctx.(mongo.SessionContext)
         .          .    341:   if !ok {
         .          .    342:           return errs.ErrConvertContext
         .          .    343:   }
         .          .    344:
         .          .    345:   db := r.client.Database(r.dbConfig.DBName)
         .          .    346:   collDNSRecord := db.Collection(r.dbConfig.Collect.DnsRecord)
         .          .    347:   collDNSRecordLifetime := db.Collection(r.dbConfig.Collect.DnsRecordLifetime)
         .          .    348:   collDNSNonHistorical := db.Collection(r.dbConfig.Collect.DnsNonHistorical)
         .          .    349:
         .          .    350:   // Is collection empty
         .          .    351:   var isDNSRecordsEmpty bool
         .      200ms    352:   isDNSRecordsEmpty, err := r.IsCollectionEmpty(ctx)
         .          .    353:   if !ok {
         .          .    354:           ctxs.AbortTransaction(ctx)
         .          .    355:           return err
         .          .    356:   }
         .          .    357:
         .          .    358:   var dnsNonHistoricals []interface{}
         .          .    359:   if !isDNSRecordsEmpty {
         .          .    360:           // Get fqdn.
         .          .    361:           record, ok := records[0].(domain.DNSRecords)
         .          .    362:           if !ok {
         .          .    363:                   ctxs.AbortTransaction(ctx)
         .          .    364:                   return errs.ErrConvertInterfaceToStruct
         .          .    365:           }
         .          .    366:
         .          .    367:           fqdn := record.Fqdn
         .          .    368:
         .          .    369:           // Search all historical records by expired_at and FQDN
         .          .    370:           // if the collection is not empty.
         .          .    371:           // Get dns_record_lifetime_id to be added to dns_non_historical.
         .          .    372:           // Later, mark them as non-historical.
         .          .    373:           /*
         .          .    374:                   db.dns_record.aggregate([
         .          .    375:                       {
         .          .    376:                           $lookup: {
         .          .    377:                               from: "dns_record_lifetime",
         .          .    378:                               localField: "_id",
         .          .    379:                               foreignField: "dns_id",
         .          .    380:                               as: "dns_record_lifetime"
         .          .    381:                           }
         .          .    382:                       },
         .          .    383:                       {
         .          .    384:                           $unwind: "$dns_record_lifetime"
         .          .    385:                       },
         .          .    386:                       {
         .          .    387:                           $lookup: {
         .          .    388:                           from: "dns_non_historical",
         .          .    389:                           let: { dns_record_lifetime_id: "$dns_record_lifetime._id" },
         .          .    390:                           pipeline: [
         .          .    391:                             {
         .          .    392:                               $match: {
         .          .    393:                                 $expr: { $eq: ["$$dns_record_lifetime_id", "$dns_record_lifetime_id"] }
         .          .    394:                               }
         .          .    395:                             },
         .          .    396:                             { $limit: 1 }
         .          .    397:                           ],
         .          .    398:                           as: "dns_non_historical"
         .          .    399:                         }
         .          .    400:                       },
         .          .    401:                       {
         .          .    402:                           $match: {
         .          .    403:                               dns_non_historical: [],
         .          .    404:                               "fqdn": DOMAIN,
         .          .    405:                               "dns_record_lifetime.created_at": { $lt: *operationStartTimePtr } // date
         .          .    406:                           }
         .          .    407:                       },
         .          .    408:                       {
         .          .    409:                           $unset: "dns_non_historical"
         .          .    410:                       },
         .          .    411:                       {
         .          .    412:                           $project: {
         .          .    413:                               _id: 0,
         .          .    414:                               dns_record_lifetime_id: "$dns_record_lifetime._id",
         .          .    415:                               a: 1,
         .          .    416:                               fqdn: 1
         .          .    417:                           }
         .          .    418:                       }
         .          .    419:                   ])
         .          .    420:           */
         .          .    421:
         .          .    422:           lookupDNSRecordLifetime := bson.D{
         .          .    423:                   {"$lookup", bson.D{
         .          .    424:                           {"from", "dns_record_lifetime"},
         .          .    425:                           {"localField", "_id"},
         .          .    426:                           {"foreignField", "dns_id"},
         .          .    427:                           {"as", "dns_record_lifetime"},
         .          .    428:                   }},
         .          .    429:           }
         .          .    430:           unwind := bson.D{
         .          .    431:                   {"$unwind", "$dns_record_lifetime"},
         .          .    432:           }
         .          .    433:           lookupDNSNonHistorical := bson.D{
         .          .    434:                   {"$lookup", bson.D{
         .          .    435:                           {"from", "dns_non_historical"},
         .          .    436:                           {"let", bson.D{
         .          .    437:                                   {"dns_record_lifetime_id", "$dns_record_lifetime._id"},
         .          .    438:                           }},
         .          .    439:                           {"pipeline", bson.A{
         .          .    440:                                   bson.D{
         .          .    441:                                           {"$match", bson.D{
         .          .    442:                                                   {"$expr", bson.D{
         .          .    443:                                                           {"$eq", bson.A{
         .          .    444:                                                                   "$$dns_record_lifetime_id",
         .          .    445:                                                                   "$dns_record_lifetime_id",
         .          .    446:                                                           }},
         .          .    447:                                                   }},
         .          .    448:                                           }},
         .          .    449:                                   },
         .          .    450:                                   bson.D{
         .          .    451:                                           {"$limit", 1},
         .          .    452:                                   },
         .          .    453:                           }},
         .          .    454:                           {"as", "dns_non_historical"},
         .          .    455:                   }},
         .          .    456:           }
         .          .    457:           match := bson.D{
         .          .    458:                   {"$match", bson.D{
         .          .    459:                           {"dns_non_historical", bson.A{}},
         .          .    460:                           {"fqdn", fqdn},
         .          .    461:                           {"dns_record_lifetime.created_at", bson.D{
         .          .    462:                                   {"$lt", *operationStartTimePtr},
         .          .    463:                           }},
         .          .    464:                   }},
         .          .    465:           }
         .          .    466:           unset := bson.D{
         .          .    467:                   {"$unset", "dns_non_historical"},
         .          .    468:           }
         .          .    469:           project := bson.D{
         .          .    470:                   {"$project", bson.D{
         .          .    471:                           {"_id", 0},
         .          .    472:                           {"dns_record_lifetime_id", "$dns_record_lifetime._id"},
         .          .    473:                           {"a", 1},
         .          .    474:                           {"fqdn", 1},
         .          .    475:                   }},
         .          .    476:           }
         .          .    477:
         .      200ms    478:           cursor, err := collDNSRecord.Aggregate(ctx, mongo.Pipeline{
         .          .    479:                   lookupDNSRecordLifetime,
         .          .    480:                   unwind,
         .          .    481:                   lookupDNSNonHistorical,
         .          .    482:                   match,
         .          .    483:                   unset,
         .          .    484:                   project,
         .          .    485:           })
         .          .    486:           if err != nil {
         .          .    487:                   ctxs.AbortTransaction(ctx)
         .          .    488:                   return err
         .          .    489:           }
         .          .    490:
         .          .    491:           // Later, mark them as non-historical.
         .          .    492:           dnsNonHistoricals = make([]interface{}, 0, cursor.RemainingBatchLength())
         .          .    493:
         .          .    494:           for cursor.Next(ctx) {
         .          .    495:                   var dnsRecordLifetimeID DNSRecordLifetimeID
         .          .    496:                   err = cursor.Decode(&dnsRecordLifetimeID)
         .          .    497:                   if err != nil {
         .          .    498:                           ctxs.AbortTransaction(ctx)
         .          .    499:                           return err
         .          .    500:                   }
         .          .    501:
         .          .    502:                   dnsNonHistoricals = append(dnsNonHistoricals, DNSNonHistorical{dnsRecordLifetimeID.ID})
         .          .    503:           }
         .          .    504:   }
         .          .    505:
         .          .    506:   // Insert new DSN records if it is not in the database.
         .          .    507:   for index, recordI := range records {
         .          .    508:           record, ok := recordI.(domain.DNSRecords)
         .          .    509:           if !ok {
         .          .    510:                   ctxs.AbortTransaction(ctx)
         .          .    511:                   return errs.ErrConvertInterfaceToStruct
         .          .    512:           }
         .          .    513:
         .          .    514:           /*
         .          .    515:                           // Try to find dns_record in database.
         .          .    516:                           db.dns_record.find(
         .          .    517:                     {
         .          .    518:                       "a": "173.194.222.103",
         .          .    519:                       "fqdn": "google.com."
         .          .    520:                     },
         .          .    521:                     {
         .          .    522:                       "_id": 1
         .          .    523:                     })
         .          .    524:           */
         .          .    525:
         .          .    526:           filter := bson.D{
         .          .    527:                   {"a", record.A},
         .          .    528:                   {"fqdn", record.Fqdn},
         .          .    529:           }
         .          .    530:           opts := options.FindOne().SetProjection(bson.M{
         .          .    531:                   "_id": 1,
         .          .    532:           })
         .          .    533:
         .          .    534:           var dnsRecordID primitive.ObjectID
         .          .    535:           var dnsRecords DNSRecords
         .      340ms    536:           err := collDNSRecord.FindOne(ctx, filter, opts).Decode(&dnsRecords)
         .          .    537:           if err == nil {
         .          .    538:                   dnsRecordID = dnsRecords.ID
         .          .    539:                   if err != nil {
         .          .    540:                           ctxs.AbortTransaction(ctx)
         .          .    541:                           return err
         .          .    542:                   }
         .          .    543:           } else {
         .          .    544:                   if !errors.Is(err, mongo.ErrNoDocuments) {
         .          .    545:                           ctxs.AbortTransaction(ctx)
         .          .    546:                           return err
         .          .    547:                   }
         .          .    548:
         .          .    549:                   // Your query did not match any documents.
         .          .    550:                   // Insert new dns_records if they are not in the database.
         .          .    551:                   doc := bson.D{
         .          .    552:                           {"a", record.A},
         .          .    553:                           {"fqdn", record.Fqdn},
         .          .    554:                   }
         .       40ms    555:                   result, err := collDNSRecord.InsertOne(ctx, doc)
         .          .    556:                   if err != nil {
         .          .    557:                           ctxs.AbortTransaction(ctx)
         .          .    558:                           return err
         .          .    559:                   }
         .          .    560:
         .          .    561:                   dnsRecordID, ok = result.InsertedID.(primitive.ObjectID)
         .          .    562:                   if !ok {
         .          .    563:                           ctxs.AbortTransaction(ctx)
         .          .    564:                           return errs.ErrConvertInterfaceToStruct
         .          .    565:                   }
         .          .    566:           }
         .          .    567:
         .          .    568:           // Insert dnsRecordId into the corresponding DNSRecordLifetime
         .          .    569:           rlt, ok := recordLifetime[index].(domain.DNSRecordLifetime)
         .          .    570:           if !ok {
         .          .    571:                   ctxs.AbortTransaction(ctx)
         .          .    572:                   return errs.ErrConvertInterfaceToStruct
         .          .    573:           }
         .          .    574:
         .          .    575:           rlt.DNSRecordID = dnsRecordID
         .          .    576:           recordLifetime[index] = rlt
         .          .    577:   }
         .          .    578:
         .          .    579:   // Insert new DSN record lifetime.
         .      130ms    580:   _, err = collDNSRecordLifetime.InsertMany(ctx, recordLifetime)
         .          .    581:   if err != nil {
         .          .    582:           ctxs.AbortTransaction(ctx)
         .          .    583:           return err
         .          .    584:   }
         .          .    585:
         .          .    586:   // Mark old records_lifetime as non-historical.
         .          .    587:   if len(dnsNonHistoricals) > 0 {
         .      100ms    588:           _, err := collDNSNonHistorical.InsertMany(ctx, dnsNonHistoricals)
         .          .    589:           if err != nil {
         .          .    590:                   ctxs.AbortTransaction(ctx)
         .          .    591:                   return err
         .          .    592:           }
         .          .    593:   }
         .          .    594:
         .       90ms    595:   if err = ctxs.CommitTransaction(ctx); err != nil {
         .          .    596:           ctxs.AbortTransaction(ctx)
         .          .    597:           return err
         .          .    598:   }
         .          .    599:
         .          .    600:   return nil

records:
reverse_dns.dns_recrods: 1774
reverse_dns.dns_record_lifetime: 1792

(pprof) list updateDNSInformation              
Total: 2.41s
ROUTINE ======================== reversedns/internal/service.(*DNSService).updateDNSInformation.func2 in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/service/dns.go
         0      190ms (flat, cum)  7.88% of Total
         .          .    138:           func(gorIndex int, fqdnsChunk []string) {
         .          .    139:                   defer func() {
         .          .    140:                           if gorCompleted.Load() < int32(gorCount-1) {
         .          .    141:                                   gorCompleted.Add(1)
         .          .    142:                           } else {
         .          .    143:                                   // All the goroutines have finished their work.
         .          .    144:                                   close(cOut)
         .          .    145:                           }
         .          .    146:                   }()
         .          .    147:
         .          .    148:                   for _, fqdn := range fqdnsChunk {
         .          .    149:                           isCanceled, err := isContextCanceled(ctx)
         .          .    150:                           if err != nil {
         .          .    151:                                   processGorErrors(err)
         .          .    152:                                   return
         .          .    153:                           }
         .          .    154:
         .          .    155:                           if isCanceled {
         .          .    156:                                   strOut := fmt.Sprintf("updateDNSInformation(): Context canceled")
         .          .    157:                                   logger.Info(strOut)
         .          .    158:                                   return
         .          .    159:                           }
         .          .    160:
         .          .    161:                           // Obtain DNS information for fqdns from the specified DNS server.
         .      160ms    162:                           dnsInfo, err := d.dns.GetDNSInfo(d.config.DNSClient.DSN, fqdn)
         .          .    163:                           if err != nil {
         .          .    164:                                   switch {
         .          .    165:                                   case os.IsTimeout(err),
         .          .    166:                                           errors.Is(err, os.ErrDeadlineExceeded):
         .          .    167:                                           cOut <- nil
         .          .    168:                                           continue
         .          .    169:                                   }
         .          .    170:
         .          .    171:                                   processGorErrors(err)
         .          .    172:                                   return
         .          .    173:                           }
         .          .    174:
         .          .    175:                           // Nothing to update
         .          .    176:                           if len(dnsInfo) < 1 {
         .       20ms    177:                                   logger.Warn("DNS server did not provide records for fqdn: " + fqdn)
         .          .    178:                                   cOut <- nil
         .          .    179:                                   continue
         .          .    180:                           }
         .          .    181:
         .          .    182:                           // Send data chunk
         .       10ms    183:                           cOut <- dnsInfo
         .          .    184:
         .          .    185:                           // TODO: Diagnostics
         .          .    186:                           iGlobalIndex++
         .          .    187:                           if iGlobalIndex >= 1000 {
         .          .    188:                                   logger.Debug("1000 point!")
ROUTINE ======================== reversedns/internal/service.(*DNSService).updateDNSInformation.func3 in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/service/dns.go
      10ms      1.26s (flat, cum) 52.28% of Total
         .          .    200:           func(gorIndex int, fqdnsChunk []string) {
         .          .    201:                   defer wg.Done()
         .          .    202:           loop:
         .          .    203:                   for {
         .          .    204:                           select {
      10ms       10ms    205:                           case dnsInfo, ok := <-cOut:
         .          .    206:                                   if !ok {
         .          .    207:                                           break loop
         .          .    208:                                   }
         .          .    209:
         .          .    210:                                   if dnsInfo == nil {
         .          .    211:                                           continue
         .          .    212:                                   }
         .          .    213:
         .          .    214:                                   // Convert DNSInfo data to model data format.
         .          .    215:                                   dnsRecords, dnsRecordLifetime := d.convertDNSInfoToModel(dnsInfo)
         .          .    216:
         .          .    217:                                   //// TODO debug:
         .          .    218:                                   //func() {
         .          .    219:                                   //      strOut := fmt.Sprintf("updateDNSInformation()\n dnsRecords = GetDNSInfo() count: %d\n", len(dnsRecords))
         .          .    220:                                   //      //for i, dnsRecord := range dnsRecords {
         .          .    221:                                   //      //      strOut = fmt.Sprintf("%sindex: %d\t %v\n", strOut, i, dnsRecord)
         .          .    222:                                   //      //}
         .          .    223:                                   //      logger.Debug(strOut)
         .          .    224:                                   //}()
         .          .    225:
         .          .    226:                                   // Update DNS info in base.
         .      1.25s    227:                                   err := d.DNSRepo.DoTransaction(ctx, func(sctx context.Context) error {
         .          .    228:                                           return d.DNSRepo.UpdateDNS(sctx, dnsRecords, dnsRecordLifetime, operationStartTimePtr)
         .          .    229:                                   })
         .          .    230:                                   if err != nil {
         .          .    231:                                           strOut := fmt.Sprintf("updateDNSInformation()\n FAILED UpdateDNS count: %d dnsRecords: %v dnsRecordLifetime: %v\nerr: %s\n", len(dnsRecords), dnsRecords, dnsRecordLifetime, err)
         .          .    232:                                           logger.Error(strOut)
ROUTINE ======================== reversedns/internal/service.(*DNSService).updateDNSInformation.func3.1 in /media/m3ik/DATA_STORAGE/0my/pr0jects/Golang/reversedns/internal/service/dns.go
         0      1.25s (flat, cum) 51.87% of Total
         .          .    227:                                   err := d.DNSRepo.DoTransaction(ctx, func(sctx context.Context) error {
         .      1.25s    228:                                           return d.DNSRepo.UpdateDNS(sctx, dnsRecords, dnsRecordLifetime, operationStartTimePtr)
         .          .    229:                                   })
         .          .    230:                                   if err != nil {
         .          .    231:                                           strOut := fmt.Sprintf("updateDNSInformation()\n FAILED UpdateDNS count: %d dnsRecords: %v dnsRecordLifetime: %v\nerr: %s\n", len(dnsRecords), dnsRecords, dnsRecordLifetime, err)
         .          .    232:                                           logger.Error(strOut)
         .          .    233:
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Blob;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
using System.Collections.Generic;
using OptionFunctions.Extensions;

namespace OptionFunctions
{
    /// <summary>
    /// brute force hacking to read a csv from blob storage
    /// and insert all records to table storage
    /// Challenge and questions
    /// - define partition and row key
    /// - cluster for bulk insert 
    /// </summary>
    public static class CsvToTableExperiment
    {
        [FunctionName("CsvToTableExperiment")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            // input file name (from query or body)
            string name = req.Query["name"];
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;
            log.LogInformation($"{nameof(CsvToTableExperiment)} triggered with {nameof(name)}:{name}");

            // Environment variables
            var CsvContainerSasUrl = Environment.GetEnvironmentVariable("CsvContainerSasUrl");
            var TableStorageSasUrl = Environment.GetEnvironmentVariable("TableStorageSasUrl");


            // 1. read csv blob
            // ---------------------
            var container = new CloudBlobContainer(new Uri(CsvContainerSasUrl));
            var blob = container.GetBlockBlobReference(name);

            using var ms = new MemoryStream();
            await blob.DownloadToStreamAsync(ms);
            ms.Seek(0, SeekOrigin.Begin);
            log.LogInformation("Downloaded stream");

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                HeaderValidated = null,
                MissingFieldFound = null
            };
            using var csv = new CsvReader(new StreamReader(ms), config);
            var records = csv.GetRecords<OptionDataRecord>().ToList(); // do I really need it to reuse the list?
            log.LogInformation("Parsed csv");

            // 2. bulk insert to Table Storage
            // -----------------------------
            // - partitionkey/rowkey???
            // - column schema


            // Table Storage
            var table = new CloudTable(new Uri(TableStorageSasUrl));

            // First iteration, sequentially, timeout with large data set
            // await InsertSquentilly(log, blob, records, table);

            // Next iteration for performance: Group by symbol and all other fields for PartitionKey to BULK INSERT
            // note: without Parallel I could insert 140k in 5min
            // note: with Parallel foreach symbol I could insert 
            var lineNumber = 0;
            var groups = records.GroupBy(record => $"us:{record.underlying_symbol}+ot:{record.option_type}");

            Parallel.ForEach(groups, group =>
           {
               log.LogInformation($"GROUP:{group.Key}");

               //Creating Batches of 100 items in order to insert them into AzureStorage  
               var batches = group.Batch(100);
               Parallel.ForEach(batches, batch =>
             {
                 var batchOperationObj = new TableBatchOperation();
                 Parallel.ForEach(batch, record =>
                 {
                     record.PartitionKey = $"us:{record.underlying_symbol}+ot:{record.option_type}";
                     record.RowKey = $"qd:{record.quote_date}+ex:{record.expiration}+ln:{lineNumber++}";
                     record.Source = blob.Uri.ToString();
                     batchOperationObj.InsertOrReplace(record);
                 });
                 table.ExecuteBatch(batchOperationObj);
             });
           });

            // output
            string responseMessage = $"CsvToTable name:{name} on env:{Environment.MachineName}. Lines:{records.Count()}";
            return new OkObjectResult(responseMessage);
        }

        private static async Task InsertSquentilly(ILogger log, CloudBlockBlob blob, List<OptionDataRecord> records, CloudTable table)
        {
            var lineNumber = 1;
            foreach (var record in records)
            {
                // TODO: Partition and Row Key definition
                // us = underlying_symbol
                // ot = option_type
                // ex = expiration
                // qd = quote_date
                // ln = lineNumber 
                record.PartitionKey = $"us:{record.underlying_symbol}+ot:{record.option_type}";
                record.RowKey = $"qd:{record.quote_date}+ex:{record.expiration}+ln:{lineNumber++}";
                record.Source = blob.Uri.ToString();

                var insertCmd = TableOperation.InsertOrMerge(record);
                await table.ExecuteAsync(insertCmd);
                log.LogDebug($"Upserted: {JsonConvert.SerializeObject(record)}");
            }
        }
    }
}

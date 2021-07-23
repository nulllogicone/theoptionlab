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

            // input
            string name = req.Query["name"];
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;
            log.LogInformation($"{nameof(CsvToTableExperiment)} triggered with {nameof(name)}:{name}");

            // 1. read csv blob
            // ---------------------

            var ContainerSasUrl = "https://optiondatafunctionstest.blob.core.windows.net/downloadcsv?sv=2020-04-08&st=2021-07-22T19%3A50%3A22Z&se=2021-08-23T19%3A50%3A00Z&sr=c&sp=rl&sig=zQ9PpvM4%2FmPihZvsbIvaJtAagJA%2BmC8EwLAxocd%2FT7E%3D";
            var container = new CloudBlobContainer(new Uri(ContainerSasUrl));
            var blob = container.GetBlockBlobReference(name);

            using var ms = new MemoryStream();
            await blob.DownloadToStreamAsync(ms);
            ms.Seek(0, SeekOrigin.Begin);
            
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                HeaderValidated = null,
                MissingFieldFound = null
            };
            using var csv = new CsvReader(new StreamReader(ms),config);
            var records =  csv.GetRecords<OptionDataRecord>().ToList();

            // 2. bulk insert to Table Storage
            // -----------------------------
            // - partitionkey/rowkey???
            // - column schema

            // group by symbol
            var ii = 0;
            var groups = records.GroupBy(r => r.underlying_symbol);
            foreach (var g in groups)
            {
                foreach (var r in g)
                {
                    log.LogInformation($"{ ii++}-{ r.underlying_symbol}");
                }
            }

            // Table Storage
            var TableStorageSasUrl = "https://optiondatafunctionstest.table.core.windows.net/optiondata?st=2021-07-22T19%3A57%3A01Z&se=2021-08-23T19%3A57%3A00Z&sp=raud&sv=2018-03-28&tn=optiondata&sig=5Qqpbjh5xjTTdpx54xqeseu6iXv%2FQjLBZCK4NkjiU8A%3D";
            var table = new CloudTable(new Uri(TableStorageSasUrl));

            var lineNumber = 1;
            foreach (var record in records)
            {
// TODO: Partition and Row Key definition
// us = underlying_symbol
// ot = option_type
// qd = quote_date
// ln = lineNumber 
                record.PartitionKey = $"us:{record.underlying_symbol}+ot:{record.option_type}";
                record.RowKey = $"qd:{record.quote_date}+ln:{lineNumber++}";
                var insertCmd = TableOperation.InsertOrMerge(record);
                await table.ExecuteAsync(insertCmd);
                log.LogDebug($"Upserted: PartitionKey:{record.PartitionKey}+RowKey:{record.RowKey}");
            }



            // output
            string responseMessage = $"CsvToTable name:{name} on env:{Environment.MachineName}. Lines:{records.Count()}";
            return new OkObjectResult(responseMessage);
        }
    }
}

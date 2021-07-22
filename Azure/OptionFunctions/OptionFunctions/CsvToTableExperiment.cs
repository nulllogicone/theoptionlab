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
            log.LogInformation("C# HTTP trigger function processed a request.");

            // input
            string name = req.Query["name"];
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            // 1. read csv blob
            // ---------------------

            var ContainerSasUrl = "https://optiondatafunctionstest.blob.core.windows.net/downloadcsv?sv=2020-04-08&st=2021-07-22T19%3A50%3A22Z&se=2021-08-23T19%3A50%3A00Z&sr=c&sp=rl&sig=zQ9PpvM4%2FmPihZvsbIvaJtAagJA%2BmC8EwLAxocd%2FT7E%3D";
            var container = new CloudBlobContainer(new Uri(ContainerSasUrl));
            var blob = container.GetBlockBlobReference(name);

            using var ms = new MemoryStream();
            await blob.DownloadToStreamAsync(ms);
            
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                HeaderValidated = null,
                MissingFieldFound = null

            };
            ms.Seek(0, SeekOrigin.Begin);
            using var csv = new CsvReader(new StreamReader(ms),config);
            var records =  csv.GetRecords<OptionDataRecord>();

            // 2. bulk insert to Table Storage
            // -----------------------------
            // - partitionkey/rowkey???
            // - column schema

            // Table Storage
            var TableStorageSasUrl = "https://optiondatafunctionstest.table.core.windows.net/optiondata?st=2021-07-22T19%3A57%3A01Z&se=2021-08-23T19%3A57%3A00Z&sp=raud&sv=2018-03-28&tn=optiondata&sig=5Qqpbjh5xjTTdpx54xqeseu6iXv%2FQjLBZCK4NkjiU8A%3D";
            var table = new CloudTable(new Uri(TableStorageSasUrl));

            var lineNumber = 1;
            foreach (var record in records)
            {
                // TODO: Partition and Row Key definition
                record.PartitionKey = $"underlying_symbol:{record.underlying_symbol}+option_type:{record.option_type}";
                record.RowKey = $"quote_date:{record.quote_date}+lineNumber{lineNumber++}";
                var insertCmd = TableOperation.InsertOrMerge(record);
                await table.ExecuteAsync(insertCmd);
            }



            // output
            string responseMessage = $"CsvToTable name:{name} on env:{Environment.MachineName}. Lines:";
            return new OkObjectResult(responseMessage);
        }
    }
}

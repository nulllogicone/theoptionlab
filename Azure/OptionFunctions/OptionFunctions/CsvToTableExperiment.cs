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
            // - example blob: https://optiondatafunctionstest.blob.core.windows.net/downloadcsv/smallset.csv?sv=2020-04-08&st=2021-07-21T19%3A52%3A05Z&se=2021-08-22T19%3A52%3A00Z&sr=b&sp=r&sig=0eUYhbU%2FbDbpqgVSQIs3qgIXHnhuGp9jeTmvvGL70h0%3D

            var BlobSasUrl = "https://optiondatafunctionstest.blob.core.windows.net/downloadcsv/smallset.csv?sv=2020-04-08&st=2021-07-21T19%3A52%3A05Z&se=2021-08-22T19%3A52%3A00Z&sr=b&sp=r&sig=0eUYhbU%2FbDbpqgVSQIs3qgIXHnhuGp9jeTmvvGL70h0%3D";
            var cloudBlockBlob = new CloudBlockBlob(new Uri(BlobSasUrl));

            var content = await cloudBlockBlob.DownloadTextAsync();
            var lines = content.Split(Environment.NewLine);
            foreach(var l in lines)
            {
                var cols = l.Split(',');
            }

            // 2. bulk insert to Table Storage
            // -----------------------------
            // - partitionkey/rowkey???
            // - column schema

            // output
            string responseMessage = $"CsvToTable name:{name} on env:{Environment.MachineName}. Lines:{lines.Length}";
            return new OkObjectResult(responseMessage);
        }
    }
}

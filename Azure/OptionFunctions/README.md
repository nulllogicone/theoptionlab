# OptionLab in the cloud

## Azure and dotnet

Goal: Host and run the code anywhere but 'not on my machine'

## Azure Functions

- CsvToTableStorage

The source of data comes from a Dropbox folder with zip files, each has 1+ Million rows. They get trasformed to csv files in a Storage account.  

This Function reads Csv and Inserts records to Table Storage (as fast as possible)

### local.settings.json

```json
{
    "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet",
    "CsvContainerSasUrl": "https://optiondatafunctionstest.blob.core.windows.net/downloadcsv?sv=2020-04-08&st=2021-07-22T19%3A50%3A22Z&se=2021-08-23T19%3A50%3A00Z&sr=c&sp=rl&sig=zQ9PpvM4%2FmPihZvsbIvaJtAagJA%2BmC8EwLAxocd%2FT7E%3D",
    "TableStorageSasUrl": "https://optionfunctions.table.core.windows.net/optiondata?st=2021-07-30T19%3A35%3A32Z&se=2021-08-02T19%3A35%3A00Z&sp=rau&sv=2018-03-28&tn=optiondata&sig=CIIUcub32%2FBARGGZBAIpkWTjYPULqja2BccCGsurO5E%3D"
  }
}
```


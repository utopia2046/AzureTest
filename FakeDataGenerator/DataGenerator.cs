using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Advertising.UX.AzureStorageUtils;

namespace FakeDataGenerator
{
    class DataGenerator
    {
        const string storageAccountConnectionString = @"DefaultEndpointsProtocol=https;AccountName=siwestusbingads;AccountKey=5+GwG1fJmBwfGkCkH64SppS+vp9DKmwvd9SO+CSUtrF5EHxGn1dxB0VXwumLaC4iAFa5JD8iIDlBQPvfJ0Y2VA==";
        const string partitionKeyFormat = "MM-dd-yyyy HH:mm:ss";
        const string message1k = @"02/02/2016 00:26:07,Account is null when perf reduce chattiness is enabled,30d6facc-c715-4d70-a5b2-a57101198462,e818a5a8-5705-437e-bdf1-fa88d8e703ab,00000000-0000-0000-0000-000000000000,0,,36113885,/Campaign/Alerts.m?aid=9343208&cid=36113885&__adcenterAJAX=true&ReqId=e818a5a8-5705-437e-bdf1-fa88d8e703ab&AdGrid.CacheId=1529f5f4-3671-9cf5-97ff-4bf2809a9f15&AdGrid.RefreshCampaignData=false&AdGrid.IsNewCacheId=true&clientCacheDataTimestamp=1454372766571&AdGrid.CacheToken=&KeywordGrid.CacheId=1529f5f4-36b1-6e03-dfd7-add81d2e4193&KeywordGrid.RefreshCampaignData=false&KeywordGrid.IsNewCacheId=true&KeywordGrid.CacheToken=,,0,Information,0,,alerts,UIRequest=alerts;Verb=GET;UrlReferrer=;UserAgent=Mozilla/5.0 (Windows NT 10.0| WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/48.0.2564.97 Safari/537.36,ThreadId=86;,,,,,CH1ADCNP500208,AdCenterCampaign,AdvertiserCampaignWebUI,ApplicationError,635899695675595002,86,Error,,ed544a1e-e425-4402-989e-cb4f6de305da,adcappcoresildc1-int-co3,adcappcoresildc1-int-co3,adcappcoresil";
        const string chars = @"ABCDEFG, HIJKLMN, OPQRST, UVWXYZ. abcdefg, hijklmn, opqrst, uvwxyz. abcdefg, hijklmn, opqrst, uvwxyz. abcdefg, hijklmn, opqrst, uvwxyz. 0123456789 -_+=:,.";

        private AzureStorageTableReaderWriter tableReaderWriter;

        public DataGenerator()
        {
            tableReaderWriter = new AzureStorageTableReaderWriter(storageAccountConnectionString);
        }

        public void GetSampleData()
        {
            var fromTime = new DateTime(2016, 2, 2);
            var toTime = fromTime + TimeSpan.FromHours(2);
            var startPartitionKey = fromTime.ToString(partitionKeyFormat);
            var endPartitionKey = toTime.ToString(partitionKeyFormat);

            var query = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, startPartitionKey),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, endPartitionKey));

            var tableRows = tableReaderWriter.FetchRowsFromTable("Error", query);
            foreach (var row in tableRows)
            {
                Console.WriteLine(row.PartitionKey);
                Console.WriteLine(row.RowKey);
                Console.WriteLine(row.Message);
            }

        }

        public void InsertFakeMessages(string tableName, int count, int batch = 1024)
        {
            Console.WriteLine("Start inserting fake data to table storage. TimeStamp: {0}", DateTime.UtcNow.ToString());
            for (int j = 0; j < count; j++)
            {
                Console.WriteLine("Batch {0} start", j);
                var stopWatch = new Stopwatch();
                var rows = new List<AzureTableRow>();
                for (int i = 0; i < batch; i++)
                {
                    var partitionKey = DateTime.UtcNow.ToString(partitionKeyFormat);
                    var rowKey = Guid.NewGuid().ToString();
                    var message = GenerateRandomString(1024);
                    var row = new AzureTableRow(partitionKey, rowKey, message);
                    rows.Add(row);
                }

                stopWatch.Start();
                tableReaderWriter.InsertMultipleRows(tableName, rows);
                stopWatch.Stop();

                TimeSpan ts = stopWatch.Elapsed;
                string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}",
                    ts.Hours, ts.Minutes, ts.Seconds,
                    ts.Milliseconds);
                Console.WriteLine("Batch {0} end. {1} rows written, time cost {2}", j, count, elapsedTime);
            }
        }

        private string GenerateRandomString(int length)
        {
            var random = new Random();
            return new string(Enumerable.Repeat(chars, length).Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}

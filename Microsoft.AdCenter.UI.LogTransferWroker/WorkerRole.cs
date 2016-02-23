using Microsoft.Advertising.UX.AzureStorageUtils;
using Microsoft.Advertising.UX.Instrumentation.Logging;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LogLevel = Microsoft.Advertising.UX.Instrumentation.Logging.LogLevel;

namespace Microsoft.AdCenter.UI.LogTransferWorker
{
    public class WorkerRole : RoleEntryPoint
    {
        // table storage partition key format
        private const string partitionKeyFormat = "MM-dd-yyyy HH:mm:ss";
        // CloudQueue.GetMessage places a lock so that it is invisible to other callers till we delete the message
        private TimeSpan queueMessageLockTimeOut = TimeSpan.FromMinutes(5);
        private const int maxRenameRetry = 100;

        private LogManager logManager;

        private string sourceStorageAccountConnectionString;
        private string targetStorageAccountConnectionString;
        private string blobContainerName;
        private string syncQueueName;

        private List<string> tableNames;
        private DateTime startTime;
        private int transferFrequencyInSecs;
        private int offsetInSecs;
        private TimeSpan frequency;
        private TimeSpan offset;

        private AzureStorageTableReaderWriter tableReader;
        private BlobStorage blobWriter;
        private QueueStorage syncQueueStorage;

        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            if (!base.OnStart())
            {
                return false;
            }

            if (!InitializeLogger())
            {
                return false;
            }

            if (!ReadConfigurations())
            {
                logManager.Log("Fail to read configuration. Please verify settings in cscfg file.", LogCategory.ApplicationError, LogLevel.Info);
                return false;
            }

            tableReader = new AzureStorageTableReaderWriter(sourceStorageAccountConnectionString);
            if (!tableReader.IsConnectionValid())
            {
                logManager.Log("Source table storage not accessible. Please verify connection string.", LogCategory.ApplicationError, LogLevel.Info);
                return false;
            }

            blobWriter = new BlobStorage(targetStorageAccountConnectionString, logManager);
            if (!blobWriter.Ping() || !blobWriter.IfBlobContainerExists(blobContainerName))
            {
                logManager.Log("Target blob storage not accessible. Please verify endpoint.", LogCategory.ApplicationError, LogLevel.Info);
                return false;
            }

            try
            {
                syncQueueStorage = new QueueStorage(sourceStorageAccountConnectionString, logManager);
            }
            catch (ArgumentNullException ex)
            {
                logManager.Log("Fail to connect with QueueStorage. Message: " + ex.Message, LogCategory.ApplicationError, LogLevel.Info);
                return false;
            }
            catch (Exception ex)
            {
                logManager.Log(ex.Message);
            }

            AddTimeSpansInQueue(startTime);
            logManager.Log("Microsoft.AdCenter.UI.LogTransferWorker is started.", LogCategory.Trace, LogLevel.Info);

            return true;
        }

        public override void OnStop()
        {
            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();
            logManager.Log("Microsoft.AdCenter.UI.LogTransferWorker is stopped.", LogCategory.Trace, LogLevel.Info);
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Check if there are more messages in the queue. If not, add one for the next time slot
                var message = GetQueueMessage();
                if (message != null)
                {
                    // gets the message from the queue and if there is no other message in the queue, then add one for the next time range to be processed.
                    var taskStartTime = Convert.ToDateTime(message.AsString);
                    var taskEndTime = taskStartTime + frequency;
                    if (IsQueueEmpty())
                    {
                        AddQueueMessage(taskEndTime.ToString(partitionKeyFormat));
                    }

                    // Make sure we are not running ahead of time, if current time is less than task end time plus offset then wait
                    if (DateTime.UtcNow < taskEndTime + offset)
                    {
                        await Task.Delay(taskEndTime + offset - DateTime.UtcNow);
                    }

                    foreach (var table in tableNames)
                    {
                        UploadLogsInTimeSlot(table, taskStartTime, taskEndTime);
                    }

                    DeleteQueueMessage(message);
                }
                // If no task in queue, wait till next time run
                else
                {
                    await Task.Delay(frequency);
                }
            }
        }

        private bool InitializeLogger()
        {
            logManager = new LogManager();

            return true;
        }

        /// <summary>
        /// Read all needed configurations
        /// if all configs are correctly load, return true
        /// if any key missing or invalid, return false
        /// </summary>
        /// <returns>True if all configs are successfully load</returns>
        private bool ReadConfigurations()
        {
            string errorTableName;
            string applicationErrorTableName;
            string traceSourceTableName;
            string performanceSourceTableName;
            string userErrorTableName;
            string latencyTableName;

            string startDateString;
            string frequencyString;
            string offSetString;

            try
            {
                sourceStorageAccountConnectionString = GetConfigKeyValue("AzureStorageConnectionString");
                targetStorageAccountConnectionString = GetConfigKeyValue("TargetStorageConnectionString");
                blobContainerName = GetConfigKeyValue("BlobContainerName");

                performanceSourceTableName = GetConfigKeyValue("PerformanceSourceTableName");
                traceSourceTableName = GetConfigKeyValue("TraceSourceTableName");
                applicationErrorTableName = GetConfigKeyValue("ApplicationErrorSourceTableName");
                userErrorTableName = GetConfigKeyValue("UserErrorSourceTableName");
                errorTableName = GetConfigKeyValue("ErrorSourceTableName");
                latencyTableName = GetConfigKeyValue("LatencySourceTableName");

                syncQueueName = GetConfigKeyValue("SynchronizationQueueName");

                startDateString = GetConfigKeyValue("StartDateInUTC");
                frequencyString = GetConfigKeyValue("TransferFrequencyInSecs");
                offSetString = GetConfigKeyValue("OffsetInSecs");
            }
            catch (InvalidOperationException ex)
            {
                logManager.Log(ex.Message, LogCategory.ApplicationError, LogLevel.Info);
                return false;
            }

            tableNames = new List<string>
            {
                errorTableName,
                applicationErrorTableName,
                traceSourceTableName,
                performanceSourceTableName,
                userErrorTableName,
                latencyTableName
            };

            startTime = ParseTimeString(startDateString);
            startTime -= (TimeSpan.FromMilliseconds(startTime.Millisecond) + TimeSpan.FromSeconds(startTime.Second));

            if (!Int32.TryParse(frequencyString, out transferFrequencyInSecs) || !Int32.TryParse(offSetString, out offsetInSecs))
            {
                logManager.Log("Fail to parse transfer frequency or offset", LogCategory.ApplicationError, LogLevel.Info);
                return false;
            }

            frequency = TimeSpan.FromSeconds(transferFrequencyInSecs);
            offset = TimeSpan.FromSeconds(offsetInSecs);

            return true;
        }

        /// <summary>
        /// Read configuration key value, if the value is null or whitespace, throw ConfigurationException
        /// </summary>
        /// <param name="key">configuration key name</param>
        /// <returns></returns>
        private string GetConfigKeyValue(string key)
        {
            string val = RoleEnvironment.GetConfigurationSettingValue(key);
            if (String.IsNullOrWhiteSpace(val))
            {
                var message = String.Format("Key {0} is null or empty. Please verify configuration.", key);
                throw new InvalidOperationException(message);
            }

            return val;
        }

        /// <summary>
        /// Try parse input date time string, if success, return the time, otherwise return UtcNow
        /// </summary>
        /// <param name="timeString">input date time string</param>
        /// <returns></returns>
        private DateTime ParseTimeString(string timeString)
        {
            DateTime time;

            if (!String.IsNullOrWhiteSpace(timeString) && DateTime.TryParse(timeString, out time))
            {
                return time;
            }

            return DateTime.UtcNow;
        }

        /// <summary>
        /// If syncQueueStorage is empty, add time spans in syncQueueStorage from start time till now
        /// </summary>
        /// <param name="startTime"></param>
        private void AddTimeSpansInQueue(DateTime startTime)
        {
            // If the queue is empty, add messages for start moving the logs starting this minute
            if (IsQueueEmpty())
            {
                DateTime time = startTime;
                do
                {
                    AddQueueMessage(time.ToString(partitionKeyFormat));
                    time += frequency;
                }
                while (time < DateTime.UtcNow);
            }
        }

        /// <summary>
        /// Read log from table in specified time slot, and upload it to target ftpClient
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="fromTime"></param>
        /// <param name="toTime"></param>
        private void UploadLogsInTimeSlot(string tableName, DateTime fromTime, DateTime toTime)
        {
            if (String.IsNullOrWhiteSpace(tableName))
            {
                return;
            }

            var startPartitionKey = fromTime.ToString(partitionKeyFormat);
            var endPartitionKey = toTime.ToString(partitionKeyFormat);

            var query = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, startPartitionKey),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, endPartitionKey));

            var tableRows = tableReader.FetchRowsFromTable(tableName, query);

            if (tableRows != null && tableRows.Any())
            {
                var blobName = GenerateBlobName(tableName, fromTime, blobContainerName);
                var logMessage = String.Format("Writing {0} records to blob {1} for time range {2} to {3}",
                    tableRows.Count(), blobName, startPartitionKey, endPartitionKey);
                logManager.Log(logMessage, LogCategory.Trace, LogLevel.Debug);

                StringBuilder sb = new StringBuilder();
                foreach (var row in tableRows)
                {
                    sb.AppendLine(row.Message);
                }

                //var stream = new System.IO.MemoryStream();

                try
                {
                    //blobWriter.AppendToBlob(blobContainerName, blobName, Encoding.UTF8.GetBytes(sb.ToString()), sb.Length);
                    blobWriter.WriteBlob(blobContainerName, blobName, stream => {
                        stream.Write(Encoding.UTF8.GetBytes(sb.ToString()), 0, sb.Length);
                    });
                }
                catch (Exception e)
                {
                    logManager.Log("Exception occured while uploading file", LogCategory.Trace, LogLevel.Info);
                    logManager.Log(e.ToString(), LogCategory.ApplicationError, LogLevel.Info);
                }
            }
            else
            {
                var logMessage = String.Format("Found 0 records in {0} for time range {1} to {2}",
                    tableName, startPartitionKey, endPartitionKey);
                logManager.Log(logMessage, LogCategory.Trace, LogLevel.Debug);
            }
        }

        private string GenerateBlobName(string tableName, DateTime startTime, string blobContainerName)
        {
            string blobName = null;
            int index;
            var fileName = String.Format("{0}_{1}__{2}",
                tableName + "Log",
                startTime.ToString("MM_dd_yyyy"),
                startTime.ToString("HH_mm"));
            var ext = GetFileExtension(tableName);

            for (index = 1; index <= maxRenameRetry; index++)
            {
                blobName = String.Format("{0}_{1}.{2}", fileName, index, ext);
                if (blobWriter.IfBlobExists(blobContainerName, blobName))
                {
                    index++;
                }
                else
                {
                    break;
                }
            }

            if (index == maxRenameRetry)
            {
                logManager.Log(String.Format("{0} duplicate logs with same name and datetime exists. Please check the storage blob {1}", maxRenameRetry, blobName),
                    LogCategory.ApplicationError, LogLevel.Info);
            }

            return blobName;
        }

        private CloudQueueMessage GetQueueMessage()
        {
            return syncQueueStorage.GetMessage(syncQueueName, queueMessageLockTimeOut);
        }

        private void DeleteQueueMessage(CloudQueueMessage message)
        {
            syncQueueStorage.DeleteMessage(syncQueueName, message);
        }

        private bool IsQueueEmpty()
        {
            return syncQueueStorage.PeekMessage(syncQueueName) == null;
        }

        private void AddQueueMessage(string message)
        {
            syncQueueStorage.AddMessage(syncQueueName, new CloudQueueMessage(message));
        }

        private string GetFileExtension(string tableName)
        {
            switch (tableName)
            {
                case "UserError":
                case "Error":
                case "ApplicationError":
                    return "errorlog.error";
                case "Performance":
                case "Latency":
                    return "perflog.perf";
                case "Trace":
                    return "tracelog.trace";
                default:
                    return "log";
            }
        }

    }
}

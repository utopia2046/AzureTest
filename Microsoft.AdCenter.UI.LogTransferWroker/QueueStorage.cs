using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Microsoft.AdCenter.UI.LogTransferWroker
{
    public interface IAzureStorageAccount
    {
        Microsoft.WindowsAzure.Storage.CloudStorageAccount GetStorageAccount();
    }

    public static class StorageHelper
    {
        public static Microsoft.WindowsAzure.Storage.OperationContext GetOperationContext()
        {
            return new Microsoft.WindowsAzure.Storage.OperationContext
            {
                ClientRequestID = Guid.NewGuid().ToString(),
                LogLevel = Microsoft.WindowsAzure.Storage.LogLevel.Warning
            };
        }
    }

    public class QueueStorage
    {
        private readonly CloudQueueClient client;
        private readonly Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount;
        private readonly ILogManager logger;
        private Dictionary<string, CloudQueue> queueRefMap;

        public QueueStorage(string connectionString, ILogManager logger)
        {
            this.client = null;
            if (logger == null)
            {
                throw new ArgumentNullException("logger");
            }

            this.logger = logger;

            if (string.IsNullOrEmpty(connectionString))
            {
                this.logger.Log("BlobStorage: Constructor is called with an empty or null connectionString", LogCategory.ApplicationError, LogLevel.Info);
                throw new ArgumentNullException(connectionString);
            }

            this.storageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);
            this.client = storageAccount.CreateCloudQueueClient();
            this.queueRefMap = new Dictionary<string, CloudQueue>();
        }

        public void AddMessage(string queueName, CloudQueueMessage message)
        {
            var operationContext = StorageHelper.GetOperationContext();
            logger.Log(String.Format("QueueStorage: Adding message to queue - queueName: {0}, RequestId: {1}",
                 queueName, operationContext.ClientRequestID), LogCategory.Trace, LogLevel.Debug);

            try
            {
                var queue = GetQueueReference(queueName);
                queue.AddMessage(message, null, null, null, operationContext);
            }
            catch (Exception e)
            {
                this.logger.Log(e.ToString(), LogCategory.ApplicationError, LogLevel.Info);
                throw;
            }
        }

        public CloudQueueMessage PeekMessage(string queueName)
        {
            var operationContext = StorageHelper.GetOperationContext();
            logger.Log(String.Format("QueueStorage: Peeking message in queue - queueName: {0}, RequestId: {1}",
                 queueName, operationContext.ClientRequestID), LogCategory.Trace, LogLevel.Debug);

            try
            {
                var queue = GetQueueReference(queueName);
                return queue.PeekMessage(null, operationContext);
            }
            catch (Exception e)
            {
                this.logger.Log(e.ToString(), LogCategory.ApplicationError, LogLevel.Info);
                throw;
            }
        }

        public CloudQueueMessage GetMessage(string queueName, TimeSpan? visibilityTimeout = null)
        {
            var operationContext = StorageHelper.GetOperationContext();
            logger.Log(String.Format("QueueStorage: Getting message from queue - queueName: {0}, RequestId: {1}",
                 queueName, operationContext.ClientRequestID), LogCategory.Trace, LogLevel.Debug);

            try
            {
                var queue = GetQueueReference(queueName);
                return queue.GetMessage(visibilityTimeout, null, operationContext);
            }
            catch (Exception e)
            {
                this.logger.Log(e.ToString(), LogCategory.ApplicationError, LogLevel.Info);
                throw;
            }
        }

        public void DeleteMessage(string queueName, CloudQueueMessage message)
        {
            var operationContext = StorageHelper.GetOperationContext();
            logger.Log(String.Format("QueueStorage: Deleting message from queue - queueName: {0}, RequestId: {1}",
                 queueName, operationContext.ClientRequestID), LogCategory.Trace, LogLevel.Debug);

            try
            {
                var queue = GetQueueReference(queueName);
                queue.DeleteMessage(message.Id, message.PopReceipt, null, operationContext);
            }
            catch (Exception e)
            {
                this.logger.Log(e.ToString(), LogCategory.ApplicationError, LogLevel.Info);
                throw;
            }
        }

        private CloudQueue GetQueueReference(string queueName)
        {
            if (!this.queueRefMap.ContainsKey(queueName))
            {
                CloudQueue queue = client.GetQueueReference(queueName);
                queue.CreateIfNotExists();
                queueRefMap.Add(queueName, queue);
            }

            return queueRefMap[queueName];
        }
    }
}

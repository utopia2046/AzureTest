namespace Microsoft.AdCenter.UI.LogTransferWroker
{
    using Microsoft.WindowsAzure.Storage.Table;
    
    /// <summary>
    /// Model representing a table entry for writing to Azure tables
    /// </summary>
    public class AzureTableRow : TableEntity
    {
        /// <summary>
        /// Empty constructor required to initialize table entity
        /// </summary>
        public AzureTableRow()
        {

        }

        /// <summary>
        /// AzureTableLogRow constructor
        /// </summary>
        /// <param name="partitionKey">Azure table parition key</param>
        /// <param name="rowKey">Azure table parition key</param>
        /// <param name="message">Formatted log message</param>
        public AzureTableRow(string partitionKey, string rowKey, string message)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Message = message;
        }
        public string Message { get; set; }
    }  
}
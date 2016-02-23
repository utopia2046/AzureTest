using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;

namespace Microsoft.AdCenter.UI.LogTransferWroker
{
    public class AzureStorageTableReaderWriter
    {
        private CloudTableClient tableClient;
        private Dictionary<string, CloudTable> tableRefMap;

        public AzureStorageTableReaderWriter(string connectionString)
        {
            Trace.TraceInformation("Fake AzureStorageTableReaderWriter initialized.");
        }

        public bool IsConnectionValid()
        {
            return true;
        }

        public IEnumerable<AzureTableRow> FetchRowsFromTable(string tableName, string queryString)
        {
            TableQuery<AzureTableRow> query = new TableQuery<AzureTableRow>()
                .Where(queryString);

            CloudTable table = GetTableReference(tableName);
            IEnumerable<AzureTableRow> azureTableRowList = table.ExecuteQuery(query);
            //IEnumerable<AzureTableRowModel> azureTableRowModelList = TransformEntityToModel(azureTableRowList);

            return azureTableRowList;
        }

        private CloudTable GetTableReference(string tableName)
        {
            CloudTable table = null;
            if (!this.tableRefMap.ContainsKey(tableName))
            {
                table = tableClient.GetTableReference(tableName);
                table.CreateIfNotExists();
                tableRefMap.Add(tableName, table);
            }

            return tableRefMap[tableName];
        }

    }
}

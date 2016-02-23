using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FakeDataGenerator
{
    class Program
    {
        string[] tableNames = { "Error", "UserError", "ApplicationError", "Trace", "Latency", "Performance" };
        const int kilo = 1024;

        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                PrintPrompt();
                return;
            }

            var tableName = args[0];
            if (!tableName.Contains(tableName))
            {
                PrintPrompt();
                return;
            }

            int size = 0;
            if (!Int32.TryParse(args[1], out size))
            {
                PrintPrompt();
                return;
            }

            DataGenerator generator = new DataGenerator();
            generator.InsertFakeMessages(tableName, size, kilo);
        }

        static void PrintPrompt()
        {
            Console.WriteLine("FakeDataGenerator.exe <TableName> <DataSizeInMB>");
            Console.WriteLine("   <TableName> = Error | UserError | ApplicationError | Trace | Latency | Performance");
            Console.WriteLine("   <DataSizeInMB> [1, 4096]");
        }
    }
}

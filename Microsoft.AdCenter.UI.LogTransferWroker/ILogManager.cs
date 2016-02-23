using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.AdCenter.Shared.Logging.ClientLibrary;

namespace Microsoft.Advertising.UX.Instrumentation.Logging
{
    public class LogManager : ILogManager
    {
        public void Log(string message, LogCategory category = LogCategory.Trace, LogLevel level = LogLevel.Info)
        {
            Trace.TraceInformation(message);
        }

        void ILogManager.BulkLog(Dictionary<LogLevel, List<IMessage>> messages)
        {
            
        }

        LogLevel ILogManager.GetLogLevel()
        {
            return LogLevel.Debug;
        }

        List<KeyValuePair<string, long>> ILogManager.GetPerformanceData()
        {
            return null;
        }

        void ILogManager.Initialize()
        {
        }

        bool ILogManager.IsActive()
        {
            return true;
        }

        void ILogManager.LogMethodCall(string message, LogCategory category, LogLevel level, string methodName, long duration, bool? isMethodEnter, Guid? transactionId, List<KeyValuePair<string, string>> additionalParams)
        {
        }

        T ILogManager.LogMethodCallWithPerf<T>(string message, LogCategory category, LogLevel level, string methodName, Func<T> method, Guid? transactionId, List<KeyValuePair<string, string>> additionalParams)
        {
            return method();
        }

        IRequestContext ILogManager.RequestContext
        {
            get { return null; }
        }

        void ILogManager.Shutdown()
        {
        }

        void ILogger.ApplicationError(string message)
        {
        }


        void ILogManager.Log(string message, LogCategory category, LogLevel level, string errorCode, int eventId, string errorSource, EventLogEntryType entryType, Guid? transactionId, List<KeyValuePair<string, string>> additionalParams)
        {
            Log(message, category, level);
        }

        void ILogManager.Log(string message, LogCategory category, LogLevel level, Guid? transactionId, List<KeyValuePair<string, string>> additionalParams)
        {
            Log(message, category, level);
        }


        void ILogger.Log(string message)
        {
            Log(message);
        }

        void ILogger.Log(Exception e, LogCategory category)
        {
            Log(e.Message, category);
        }

        void ILogger.Log(Exception e)
        {
            Log(e.Message);
        }
    }

}

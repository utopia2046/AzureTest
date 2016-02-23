using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Advertising.UX.Instrumentation.Logging;

namespace Microsoft.AdCenter.UI.LogTransferWroker
{
    public class FtpHelper
    {
        private string ftpUrl;
        private ILogManager logManager;

        public FtpHelper(string ftpUrl, ILogManager logManager)
        {
            this.ftpUrl = ftpUrl;
            this.logManager = logManager;
            Trace.TraceInformation("Fake FtpHelper initialized.");
        }

        public bool VerifyEndpoint()
        {
            return true;
        }

        public void UploadFile(string directoryName, string fileName, byte[] fileContents)
        {
            System.Threading.Thread.Sleep(1000);
            Trace.TraceInformation("Fake FtpHelper UploadFile done.");
        }
    }
}

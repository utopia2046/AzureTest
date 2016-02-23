using System;
using System.IO;
using System.Net;
using Microsoft.Advertising.UX.Instrumentation.Logging;

namespace Microsoft.AdCenter.UI.LogTransferWroker
{
    public class FtpHelper
    {
        private readonly string url;
        private readonly ILogManager logManager;
        private const string testFileName = "verify.txt";

        public FtpHelper(string ftpUrl, ILogManager logManager)
        {
            if (!ftpUrl.EndsWith("/"))
            {
                ftpUrl += "/";
            }

            this.url = ftpUrl;
            this.logManager = logManager;
        }

        public bool VerifyEndpoint()
        {
            var request = (FtpWebRequest)WebRequest.Create(url + testFileName);
            request.Method = WebRequestMethods.Ftp.UploadFile;

            try
            {
                var response = (FtpWebResponse)request.GetResponse();
                response.Close();
                return true;
            }
            catch (Exception e)
            {
                if (logManager != null)
                {
                    logManager.Log(String.Format("FTP site with url {0} is not accessible. Please verify url or permissions. {1}", this.url, e.ToString()), LogCategory.ApplicationError, LogLevel.Info);
                }

                return false;
            }
        }

        public bool UploadFile(string directoryName, string fileName, byte[] fileContents)
        {
            FtpWebRequest request = null;
            var path = url;
            if (!string.IsNullOrWhiteSpace(directoryName))
            {
                path += directoryName;
            }

            path += "/" + fileName;

            try
            {
                request = (FtpWebRequest) WebRequest.Create(path);
                request.Method = WebRequestMethods.Ftp.UploadFile;

                //Copy file contents to request stream
                Stream requestStream = request.GetRequestStream();
                requestStream.Write(fileContents, 0, fileContents.Length);
                requestStream.Close();

                var response = (FtpWebResponse) request.GetResponse();
                response.Close();
                return true;
            }
            catch (WebException)
            {
                if (logManager != null)
                {
                    logManager.Log("Creating directory: " + directoryName, LogCategory.Trace, LogLevel.Info);
                }

                // the exception might be due to directory not existing; try creating the directory and then add the file.
                var createDirectoryRequest = (FtpWebRequest) WebRequest.Create(url + directoryName);
                createDirectoryRequest.Method = WebRequestMethods.Ftp.MakeDirectory;
                var createDirectoryResponse = (FtpWebResponse) createDirectoryRequest.GetResponse();
                createDirectoryResponse.Close();

                //retry uploading the file again
                if (request != null)
                {
                    var response = (FtpWebResponse) request.GetResponse();
                    response.Close();
                    return true;
                }

                return false;
            }
            catch (Exception e)
            {
                if (logManager != null)
                {
                    logManager.Log(String.Format("Unable to upload file {0}", fileName, e.ToString()),
                        LogCategory.ApplicationError, LogLevel.Info);
                }

                return false;
            }
        }
    }
}

using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;

public class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Starting the file processing...");

        // Call the ProcessPaxFiles method and wait for it to complete
        FTPDownloadAndUpload.ProcessPaxFiles().Wait();

        Console.WriteLine("File processing completed.");
    }
}

public class FTPDownloadAndUpload
{
    public static async Task ProcessPaxFiles()
    {
        string ftpUserID = "Nagarro";
        string ftpPassword = "Dm&XxeaRN+1Y";

        // Paths for original FTP downloads
        string ftpServerIP1 = "ftp://sunway-ttss.northeurope.cloudapp.azure.com/Paxport/paxcache-sunway.csv.gz";
        string ftpServerIP2 = "ftp://sunway-ttss.northeurope.cloudapp.azure.com/RyanairTTSS/ttss-ie-ryanair-returns.csv.gz";

        string localPath1 = @"D:\flights\PaxFileOriginal\paxcache-sunway.csv.gz";
        string localPath2 = @"D:\flights\PaxFileOriginal\ttss-ie-ryanair-returns.csv.gz";

        // Step 1: Download both files
        await DownloadFileFromFTP(ftpServerIP1, ftpUserID, ftpPassword, localPath1);
        await DownloadFileFromFTP(ftpServerIP2, ftpUserID, ftpPassword, localPath2);

        // Step 2: Extract both files
        string extractedFile1 = ExtractGzipFile(localPath1);
        string extractedFile2 = ExtractGzipFile(localPath2);

        // Step 3: Modify and append content
        AppendContentWithModification(extractedFile2, extractedFile1);

        // Step 4: Compress the final combined file into .gz
        string finalCompressedFile = CompressToGzip(extractedFile1);

        // Step 5: Upload the final .gz file to a different FTP server
        // Step 5: Create the completed file if compression was successful
        if (File.Exists(finalCompressedFile))
        {
            string completedFile = finalCompressedFile + ".completed";
            File.Create(completedFile).Dispose(); // Creates the .completed file

            // Step 6: Upload the final .gz file to a different FTP server
            string destinationFtpUri = "ftp://sunway-ttss.northeurope.cloudapp.azure.com/Paxport/combined_paxcache.csv.gz";
            await UploadFileToFTP(finalCompressedFile, destinationFtpUri, ftpUserID, ftpPassword);

            Console.WriteLine($"Uploaded {finalCompressedFile} and created {completedFile}.");
        }
        else
        {
            Console.WriteLine("Compression failed, .completed file not created.");
        }
    }

    static async Task DownloadFileFromFTP(string ftpUri, string user, string password, string localFilePath)
    {
        try
        {
            Uri serverUri = new Uri(ftpUri);
            if (serverUri.Scheme != Uri.UriSchemeFtp)
            {
                throw new InvalidOperationException("URI scheme is not FTP.");
            }

            FtpWebRequest reqFTP = (FtpWebRequest)WebRequest.Create(serverUri);
            reqFTP.Credentials = new NetworkCredential(user, password);
            reqFTP.KeepAlive = false;
            reqFTP.Method = WebRequestMethods.Ftp.DownloadFile;
            reqFTP.UseBinary = true;
            reqFTP.Proxy = null;
            reqFTP.UsePassive = false;

            using FtpWebResponse response = (FtpWebResponse)await reqFTP.GetResponseAsync();
            using Stream responseStream = response.GetResponseStream();
            using FileStream writeStream = new FileStream(localFilePath, FileMode.Create);

            byte[] buffer = new byte[2048];
            int bytesRead;
            while ((bytesRead = await responseStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                await writeStream.WriteAsync(buffer, 0, bytesRead);
            }

            Console.WriteLine($"Downloaded file from {ftpUri} to {localFilePath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Download Error: {ex.Message}");
        }
    }

    static string ExtractGzipFile(string compressedFilePath)
    {
        string extractedFilePath = compressedFilePath.Replace(".gz", "");
        try
        {
            using FileStream originalFileStream = new FileStream(compressedFilePath, FileMode.Open);
            using FileStream decompressedFileStream = new FileStream(extractedFilePath, FileMode.Create);
            using GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress);
            decompressionStream.CopyTo(decompressedFileStream);

            Console.WriteLine($"Extracted {compressedFilePath} to {extractedFilePath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Extraction Error: {ex.Message}");
        }

        return extractedFilePath;
    }

    static void AppendContentWithModification(string sourceFile, string destinationFile)
    {
        try
        {
            if (File.Exists(sourceFile))
            {
                string sourceContent = File.ReadAllText(sourceFile);
                string modifiedContent = sourceContent.Replace("RYAN", "RYR");
                File.AppendAllText(destinationFile, modifiedContent);

                Console.WriteLine($"Modified contents of {sourceFile} appended to {destinationFile}.");
            }
            else
            {
                Console.WriteLine($"Source file {sourceFile} does not exist.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred: " + ex.Message);
        }
    }

    static string CompressToGzip(string filePath)
    {
        string compressedFilePath = filePath + ".gz";
        try
        {
            using FileStream originalFileStream = new FileStream(filePath, FileMode.Open);
            using FileStream compressedFileStream = new FileStream(compressedFilePath, FileMode.Create);
            using GZipStream compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress);
            originalFileStream.CopyTo(compressionStream);

            Console.WriteLine($"Compressed {filePath} to {compressedFilePath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Compression Error: {ex.Message}");
        }

        return compressedFilePath;
    }

    static async Task UploadFileToFTP(string filePath, string ftpUri, string user, string password)
    {
        try
        {
            Uri serverUri = new Uri(ftpUri);
            if (serverUri.Scheme != Uri.UriSchemeFtp)
            {
                throw new InvalidOperationException("URI scheme is not FTP.");
            }

            FtpWebRequest reqFTP = (FtpWebRequest)WebRequest.Create(serverUri);
            reqFTP.Credentials = new NetworkCredential(user, password);
            reqFTP.KeepAlive = false;
            reqFTP.Method = WebRequestMethods.Ftp.UploadFile;
            reqFTP.UseBinary = true;
            reqFTP.Proxy = null;
            reqFTP.UsePassive = false;

            using FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            using Stream requestStream = await reqFTP.GetRequestStreamAsync();
            byte[] buffer = new byte[2048];
            int bytesSent;
            while ((bytesSent = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                await requestStream.WriteAsync(buffer, 0, bytesSent);
            }

            Console.WriteLine($"Uploaded {filePath} to {ftpUri}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Upload Error: {ex.Message}");
        }
    }
}
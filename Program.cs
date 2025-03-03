using Microsoft.VisualBasic.FileIO;
using System;
using System.Globalization;
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

        //// Paths for original FTP downloads
        string ftpBaseUri1 = "ftp://sunway-ttss.northeurope.cloudapp.azure.com/CalendarFiles/Paxport/PaxPort/";
        string ftpBaseUri2 = "ftp://sunway-ttss.northeurope.cloudapp.azure.com/CalendarFiles/Ryanair/RyanairTTSS/";

        string paxFileToDownload = "paxcache-sunway.csv.gz";
        string ryanairFileToDownload = "ttss-ie-ryanair-returns.csv.gz";

        string localPath1 = @"D:\flights\paxcache-sunway.csv.gz";
        string localPath2 = @"D:\flights\ttss-ie-ryanair-returns.csv.gz";


        try
        {
            //// Step 1: Download both files
            await DownloadFileFromFTP(ftpBaseUri1, ftpUserID, ftpPassword, localPath1, paxFileToDownload);
            await DownloadFileFromFTP(ftpBaseUri2, ftpUserID, ftpPassword, localPath2, ryanairFileToDownload);

            if (!File.Exists(localPath1) || !File.Exists(localPath2))
            {
                Console.WriteLine("Download failed for one or both files. Exiting process.");
                return;
            }

            //// Step 2: Extract both files
            string extractedFile1 = ExtractGzipFile(localPath1);
            string extractedFile2 = ExtractGzipFile(localPath2);

            if (!File.Exists(extractedFile1) || !File.Exists(extractedFile2))
            {
                Console.WriteLine("Extraction failed for one or both files. Exiting process.");
                return;
            }

            //// Step 3: Modify and append content
            AppendContentWithModification(extractedFile2, extractedFile1);

            //string extractedFile1 = @"D:\flights\paxcacheshort.csv";

            //AddDiscreetFares(extractedFile1);
            // Step 4: Compress the final combined file into .gz
            string compressToPath = "D:\\flights\\final\\paxcache-sunway.csv";
            string finalCompressedFile = CompressToGzip(extractedFile1, compressToPath);

            // Step 5: Upload the final .gz file to a different FTP server
            // Step 5: Create the completed file if compression was successful
            if (File.Exists(finalCompressedFile))
            {
                string completedFile = finalCompressedFile + ".completed";
                File.Create(completedFile).Dispose(); // Creates the .completed file
                string paxFtpUserID = "sunway";
                string paxFtpPassword = "Xuox0eazJoow4ge8";

                // Step 6: Upload the final .gz file to a different FTP server
                string paxDestinationFtpUri = "ftp://ftp.multicom.co.uk/dev/testupload/paxcache-sunway.csv.gz";


                bool uploadSuccess = await UploadFileToFTP(finalCompressedFile, paxDestinationFtpUri, paxFtpUserID, paxFtpPassword);
                bool uploadCompletedSuccess = await UploadFileToFTP(completedFile, paxDestinationFtpUri + ".completed", paxFtpUserID, paxFtpPassword);

                if (uploadSuccess && uploadCompletedSuccess)
                {
                    Console.WriteLine("Files uploaded successfully.");

                    // Step 7: Delete local files after successful upload
                    DeleteLocalFiles(new[] { localPath1, localPath2, extractedFile1, extractedFile2, finalCompressedFile, completedFile });
                }
                else
                {
                    Console.WriteLine("File upload failed. Local files will not be deleted.");
                }

                Console.WriteLine($"Uploaded {finalCompressedFile} and created {completedFile}.");
            }
            else
            {
                Console.WriteLine("Compression failed, .completed file not created.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }



    }

    static async Task<bool> DownloadFileFromFTP(string ftpBaseUri, string user, string password, string localFilePath, string fileToDownload)
    {
        try
        {
            // Step 1: Connect to the FTP server and list directory contents
            FtpWebRequest listRequest = (FtpWebRequest)WebRequest.Create(ftpBaseUri);
            listRequest.Credentials = new NetworkCredential(user, password);
            listRequest.Method = WebRequestMethods.Ftp.ListDirectory;
            listRequest.UsePassive = true;
            listRequest.KeepAlive = false;

            List<string> directories = new List<string>();

            using (FtpWebResponse listResponse = (FtpWebResponse)await listRequest.GetResponseAsync())
            using (StreamReader reader = new StreamReader(listResponse.GetResponseStream()))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    directories.Add(line);
                }
            }

            // Step 2: Identify today's folder based on the date prefix
            string todayPrefix = DateTime.Now.ToString("yyyyMMdd");
            string todayFolder = directories.FirstOrDefault(dir => dir.StartsWith(todayPrefix));

            if (string.IsNullOrEmpty(todayFolder))
            {
                Console.WriteLine("No folder found for today's date.");
                return false;
            }

            // Step 3: Construct the FTP URI for the file inside today's folder
            string fileUri = $"{ftpBaseUri}{todayFolder}/{fileToDownload}";
            Console.WriteLine($"Attempting to download file from: {fileUri}");

            // Step 4: Download the file
            FtpWebRequest downloadRequest = (FtpWebRequest)WebRequest.Create(fileUri);
            downloadRequest.Credentials = new NetworkCredential(user, password);
            downloadRequest.Method = WebRequestMethods.Ftp.DownloadFile;
            downloadRequest.UseBinary = true;
            downloadRequest.Proxy = null;
            downloadRequest.UsePassive = true;
            downloadRequest.KeepAlive = false;

            using (FtpWebResponse response = (FtpWebResponse)await downloadRequest.GetResponseAsync())
            using (Stream responseStream = response.GetResponseStream())
            using (FileStream fileStream = new FileStream(localFilePath, FileMode.Create, FileAccess.Write))
            {
                byte[] buffer = new byte[2048];
                int bytesRead;
                while ((bytesRead = await responseStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    await fileStream.WriteAsync(buffer, 0, bytesRead);
                }
            }

            Console.WriteLine($"Downloaded file to {localFilePath}");

            return true; // Download and deletion succeeded
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during download or deletion from FTP: {ex.Message}");
            return false; // Download or deletion failed
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




                //Console.WriteLine($"Modified contents of {sourceFile} appended to {destinationFile}.");

                //string destinationFileString = File.ReadAllText(destinationFile);

                try
                {
                    string[] lines = File.ReadAllLines(destinationFile);

                    for (int i = 0; i < lines.Length; i++)
                    {
                        string[] _line = lines[i].Split(",");
                        string sanitizedString = _line[67].Trim('"');
                        if (_line[6] != _line[7])
                        {
                            if (int.TryParse(sanitizedString, out int number))
                            {
                                if(number != 0)
                                {
                                    number += 1;
                                    string updatedString = $"\"{number}\"";
                                    _line[67] = updatedString;
                                }                                
                            }
                        }


                        //Console.WriteLine(line); // Process each line as needed
                    }
                    File.WriteAllLines(destinationFile, lines);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }

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

    static string AddDiscreetFares(string path)
    {
        string criteriaFile = "D:\\flights\\criteria_file.txt";
        // Read all lines from both files
        //string[] flightLines = File.ReadAllLines(path);
        //string[] criteriaLines = File.ReadAllLines("D:\\flights\\criteria_file.txt");

        // Load CSV file
        List<string[]> flightData = new List<string[]>();
        using (TextFieldParser csvParser = new TextFieldParser(path))
        {
            csvParser.TextFieldType = FieldType.Delimited;
            csvParser.SetDelimiters(",");

            while (!csvParser.EndOfData)
            {
                // Read each line and add to list as an array of strings
                flightData.Add(csvParser.ReadFields());
            }
        }

        // Load criteria file (assuming plain text, comma-separated)
        List<string[]> criteriaData = new List<string[]>();
        foreach (var line in File.ReadAllLines(criteriaFile))
        {
            criteriaData.Add(line.Split(',')); // Split each line by commas
        }

        // Loop through flight data and apply criteria checks
        for (int i = 0; i < flightData.Count - 3; i++)
        {
            string paxDate = flightData[i + 1][7];
            // Parse both date strings into DateTime objects
            DateTime parsedPaxDate = DateTime.ParseExact(paxDate, "dd/MM/yyyy", CultureInfo.InvariantCulture);

            foreach (var criteria in criteriaData)
            {

                string discreetDate = criteria[3];
                DateTime parsedDiscreetDate = DateTime.ParseExact(discreetDate, "yyyyMMdd", CultureInfo.InvariantCulture);

                // Adjust column indices as necessary to match your criteria exactly
                if (flightData[i + 1][1] == criteria[0].Trim() &&         // Dep Airport
                    flightData[i + 1][2] == criteria[1].Trim() &&         // Arrival Airport
                    flightData[i + 1][67] == criteria[4] &&  // Duration
                    parsedPaxDate == parsedDiscreetDate &&              // 
                    flightData[i + 4][5] == criteria[5].Trim())                // Airline
                {
                    // Update row 69 and 70 in the current row with criteria data (assuming column 3 in criteria for update)
                    flightData[i][68] = criteria[2];
                    flightData[i][69] = criteria[2];
                }
            }
        }
        string newFile = "D:\\flights\\updated_flight_prices.csv";

        // Save the updated data back to a CSV file
        using (StreamWriter writer = new StreamWriter(newFile))
        {
            foreach (var row in flightData)
            {
                writer.WriteLine(string.Join(",", row));
            }
        }

        return newFile;
    }


    static string CompressToGzip(string filePath, string compressToPath)
    {
        string compressedFilePath = compressToPath + ".gz";
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

    static async Task<bool> UploadFileToFTP(string filePath, string ftpUri, string user, string password)
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
            return true; // Upload succeeded
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Upload Error for {filePath} to {ftpUri}: {ex.Message}");
            return false; // Upload failed
        }
    }

    // Helper method to delete local files
    static void DeleteLocalFiles(string[] filePaths)
    {
        foreach (var filePath in filePaths)
        {
            if (File.Exists(filePath))
            {
                try
                {
                    File.Delete(filePath);
                    Console.WriteLine($"Deleted local file: {filePath}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to delete file {filePath}: {ex.Message}");
                }
            }
        }
    }
}
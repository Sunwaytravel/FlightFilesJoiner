using System;
using System.IO;

class Program
{
    static void Main(string[] args)
    {


        // Define the paths of the source and destination files
        string sourceFile = @"C:\Users\js\OneDrive - sunway.ie\Documents\paxport\ttss-ie-ryanair-returns.csv";
        string destinationFile = @"C:\Users\js\OneDrive - sunway.ie\Documents\paxport\paxcache-sunway.csv";


        try
        {
            // Check if the source file exists
            if (File.Exists(sourceFile))
            {
                // Read the contents of the source file
                string sourceContent = File.ReadAllText(sourceFile);

                string modifiedContent = sourceContent.Replace("RYAN", "RYR");


                // Append the modified content to the destination file
                File.AppendAllText(destinationFile, modifiedContent);

                // Append the contents to the destination file
                //File.AppendAllText(destinationFile, sourceContent);

                Console.WriteLine($"Contents of {sourceFile} have been appended to {destinationFile}.");
            }
            else
            {
                Console.WriteLine($"Source file {sourceFile} does not exist.");
            }
        }
        catch (Exception ex)
        {
            // Handle any exceptions that might occur during file operations
            Console.WriteLine("An error occurred: " + ex.Message);
        }
    }
}

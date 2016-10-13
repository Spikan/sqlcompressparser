using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using ICSharpCode.SharpZipLib.Core;
using ICSharpCode.SharpZipLib.Zip;

namespace pfboolparse
{
    internal static class Program
    {
        private static readonly string Path = Directory.GetCurrentDirectory() + "\\psvs\\";
        private static readonly string Db = ConfigurationManager.AppSettings["Database"];

        private static void Main(string[] args)
        {
           
            
            var exists = Directory.Exists(Path);
            if (!exists)
                Directory.CreateDirectory(Path);

            var taxonomy = new SqlConnection
            {
                ConnectionString = "Data Source=" + Db + ";" +
                                   "Initial Catalog=segment_taxonomy;" +
                                   "User id=sa;" +
                                   "Password=liamcow;"
            };

            


            taxonomy.Open();

            var tableList = new List<string>();

            using (var command = taxonomy.CreateCommand())
            {
                command.CommandTimeout = int.MaxValue;
                command.CommandText =
                   ConfigurationManager.AppSettings["Query1"];
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tableList.Add(reader["Table_Name"].ToString());
                        //Console.WriteLine(reader["Table_Name"]);
                    }
                }
            }

            taxonomy.Close();


            var threads = args.Length != 0 ? Convert.ToInt32(args[0]) : Convert.ToInt32(ConfigurationManager.AppSettings["Threads"]);

            var tablesplit = tableList.Split(threads);


            foreach (var thread in tablesplit.Select(ll => new Thread(() => DoGrab(ll))))
            {
                thread.Start();
            }

        }

        private static void DoGrab<T>(IEnumerable<T> list)
        {
            Console.SetOut(Console.Out);
            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Thread  #" + Thread.CurrentThread.ManagedThreadId + " Started");

            var sqlServer = "-S " + Db;
            foreach (var s in list)
            {
                var query2 = ConfigurationManager.AppSettings["Query2"];
                var replace = "[" + s + "]";
                
                query2 = query2.Replace("TABLENAME", replace);
                ProcessCmd("bcp",
                    $"\"{query2}\" queryout \"{Path}{s}.psv\" -c -t | -U sa -P liamcow {sqlServer}", s.ToString());

                Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Thread #" + Thread.CurrentThread.ManagedThreadId + " is working on " + s);
            }
            Console.WriteLine("***" + DateTime.Now.ToLongTimeString() + ": Thread #" + Thread.CurrentThread.ManagedThreadId + " has completed***");
            /*
            var liverampExport = new SqlConnection
            {
                ConnectionString = "Data Source=" + Db + ";" +
                                   "Initial Catalog=liveramp_export_2016_10;" +
                                   "User id=sa;" +
                                   "Password=liamcow;"
            };

            var ids = new List<string>();
            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Thread  #" + Thread.CurrentThread.ManagedThreadId + " Started");
            liverampExport.Open();

            foreach (var s in list)
            {
                
                using (var command = liverampExport.CreateCommand())
                {
                    var query = ConfigurationManager.AppSettings["Query2"];
                    var replace = "\"" + s + "\"";
                    query = query.Replace("TABLENAME", replace);
                    command.CommandTimeout = int.MaxValue;
                    command.CommandText = query;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            //tableList.Add(reader["Table_Name"].ToString());
                            ids.Add(reader["pf_id"].ToString());
                            //Console.WriteLine(s + ": " + reader["pf_id"]);
                        }
                    }


                    
                        foreach (var ss in ids)
                        {
                            using (var sw = new StreamWriter(Path + s + ".psv"))
                            {
                                sw.WriteLine(ss + "\r\n");
                            }
                        }
                    

                    //var strings = ids.Aggregate<string, string>(null, (current, ss) => current + (ss + "\r\n"));


                    //File.WriteAllText(Path + s + ".psv", strings);
                    ids.Clear();
                }

            }

            liverampExport.Close();
            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": ***Thread #" + Thread.CurrentThread.ManagedThreadId + " has completed***");
            */
        }

        private static void ProcessCmd(string fileName, string arguments, string zfile)
        {

            var proc = new System.Diagnostics.Process
            {
                StartInfo =
                {
                    FileName = fileName,
                    Arguments = arguments,
                    UseShellExecute = false,
                    RedirectStandardOutput = true
                }
            };
            proc.Start();
            proc.OutputDataReceived += null;
            proc.BeginOutputReadLine();
            proc.WaitForExit();

            //using (var zip = ZipFile.Open(Path + zfile + ".zip", ZipArchiveMode.Create))
            //    zip.CreateEntryFromFile(Path + zfile + ".psv", zfile + ".psv");
            CreateZip(zfile);
            File.Delete(Path + zfile + ".psv");
        }
        
        public static void CreateZip(string filename)
        {
            var fsOut = File.Create(Path + filename + ".zip");
            var zipStream = new ZipOutputStream(fsOut);

            zipStream.SetLevel(9); //0-9, 9 being the highest level 

            var fi = new FileInfo(Path + filename + ".psv");

            var entryName = filename + ".psv"; // Makes the name in zip based on the folder
            entryName = ZipEntry.CleanName(entryName); // Removes drive from name and fixes slash direction
            var newEntry = new ZipEntry(entryName)
            {
                DateTime = fi.LastWriteTime,
                Size = fi.Length
            };
            // Note the zip format stores 2 second granularity

            // Specifying the AESKeySize triggers AES encryption. Allowable values are 0 (off), 128 or 256.
            // A password on the ZipOutputStream is required if using AES.
            //   newEntry.AESKeySize = 256;

            // To permit the zip to be unpacked by built-in extractor in WinXP and Server2003, WinZip 8, Java, and other older code,
            // you need to do one of the following: Specify UseZip64.Off, or set the Size.
            // If the file may be bigger than 4GB, or you do not need WinXP built-in compatibility, you do not need either,
            // but the zip will be in Zip64 format which not all utilities can understand.
            //   zipStream.UseZip64 = UseZip64.Off;

            zipStream.PutNextEntry(newEntry);

            // Zip the file in buffered chunks
            // the "using" will close the stream even if an exception occurs
            var buffer = new byte[4096];
            using (var streamReader = File.OpenRead(Path + filename + ".psv"))
            {
                StreamUtils.Copy(streamReader, zipStream, buffer);
            }

            zipStream.IsStreamOwner = true; // Makes the Close also Close the underlying stream
            zipStream.Close();
        }
        
    }

    
}

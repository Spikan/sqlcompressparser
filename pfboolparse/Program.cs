using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Remoting.Metadata.W3cXsd2001;
using System.Threading;
using ICSharpCode.SharpZipLib.Core;
using ICSharpCode.SharpZipLib.Zip;

namespace pfboolparse
{
    internal static class Program
    {
        //path for psvs
        private static readonly string Path = Directory.GetCurrentDirectory() + "\\psvs\\";
        //Get data from app.config
        private static readonly string Un = ConfigurationManager.AppSettings["Username"];
        private static readonly string Ps = ConfigurationManager.AppSettings["Password"];
        private static readonly string Sv = ConfigurationManager.AppSettings["Server"];
        private static readonly string Db = ConfigurationManager.AppSettings["Database"]; 

        private static void Main(string[] args)
        {
            string server;
            string database;
            var user = Un;
            var pass = Ps;

            //ask if user wants to use values from app.config
            Console.Write("Use Settings From Config? y/n: "); 

            var config = Console.ReadLine();
            if (config != null && char.ToUpper(config[0]) == 'N')
            {
                //get user inputted constraints, Server, DB
                Console.Write("Server?: ");
                server = Console.ReadLine();
                Console.WriteLine(server);
                Console.Write("Database?: ");
                database = Console.ReadLine();
                Console.WriteLine("Server: " + server + "\r\n DB: " + database); 
                
            }
            else
            {
                //else pull data from app.config
                server = Sv;
                database = Db; 
            }

            var exists = Directory.Exists(Path);
            var exists1 = Directory.Exists(Path + "segment_as_column\\");
            var exists2 = Directory.Exists(Path + "filewide\\");

            //look for \psvs\ path, and create if doesn't exist
            if (!exists)
                Directory.CreateDirectory(Path);  

            if (!exists1)
                Directory.CreateDirectory(Path + "segment_as_column\\");

            if (!exists2)
                Directory.CreateDirectory(Path + "filewide\\");

            //create new sql connection to server and db
            var connection = new SqlConnection  
            {
                ConnectionString = "Data Source=" + server + ";" +
                                   "Initial Catalog=" + database + ";" +
                                   "User id=" + user + ";" +
                                   "Password=" + pass + ";"
            };

            //open connection
            connection.Open();

            //create list to store table names
            var tableList = new List<string>();

            //create query command
            using (var command = connection.CreateCommand()) 
            {
                //timeout max
                command.CommandTimeout = int.MaxValue;
                //read query from app.config
                command.CommandText =
                   ConfigurationManager.AppSettings["Query"];
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        //add each table name to tableList
                        tableList.Add(reader["Table_Name"].ToString()); 
                        //Console.WriteLine(reader["Table_Name"]);
                    }
                }
            }
            //close connection
            connection.Close(); 

            //determine # of threads either in app.config or command line arg
            var threads = args.Length != 0 ? Convert.ToInt32(args[0]) : Convert.ToInt32(ConfigurationManager.AppSettings["Threads"]);

            //split list of tables into groups for threads to handle
            var tablesplit = tableList.Split(threads); 


            foreach (var thread in tablesplit.Select(ll => new Thread(() => DoGrab(ll, server, database, user, pass))))
            {
                //start data grab
                thread.Start(); 
            }

        }

        private static void DoGrab<T>(IEnumerable<T> list, string server, string database, string user, string pass)
        {
            Console.SetOut(Console.Out);
            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Thread  #" + Thread.CurrentThread.ManagedThreadId + " Started");

            
            var connection2 = new SqlConnection
            {
                ConnectionString = "Data Source=" + server + ";" +
                                   "Initial Catalog=" + database + ";" +
                                   "User id=" + user + ";" +
                                   "Password="+ pass + ";"
            };

            var ids = new List<string>();
            connection2.Open();

                
               

            
            

            
            foreach (var s in list)
            {
                /*

                using (var command = liverampExport.CreateCommand())
                {                   
                    var query3 = ConfigurationManager.AppSettings["Query3"];
                    query3 = query3.Replace("TABLENAME", s.ToString());
                    command.CommandTimeout = int.MaxValue;
                    command.CommandText = query3;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            //tableList.Add(reader["Table_Name"].ToString());
                            ids.Add(reader["pf_id"].ToString());
                            //Console.WriteLine(s + ": " + reader["pf_id"]);
                        }
                    }



                    var strings = ids.Aggregate<string, string>(null, (current, ss) => current + (ss + "\r\n"));


                    
                    ids.Clear();


                }

                liverampExport.Close();
                */
                var colNum = GetColNum(connection2, "" + s);
                var query1 = ConfigurationManager.AppSettings["1ColQuery"];
                var query2 = ConfigurationManager.AppSettings["2ColQuery"];

                if (colNum == 1)
                {
                    File.WriteAllText(Path + "filewide\\" + s, "pf_id" + "\r\n");
                    DoBcp(Path + "filewide\\", "" + s, query1, database, server, colNum, connection2);
                }
                else
                {
                    File.WriteAllText(Path + "segment_as_column\\" + s, "pf_id|" + s + "\r\n");
                    DoBcp(Path + "segment_as_column\\", "" + s, query2, database, server, colNum, connection2);
                }

            }
            Console.WriteLine("***" + DateTime.Now.ToLongTimeString() + ": Thread #" + Thread.CurrentThread.ManagedThreadId + " has completed***");
            connection2.Close();
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
            //    zip.CreateEntryFromFile(Path + zfile, zfile);
            if (zfile == "0") return;
            //CreateZip(zfile);
            File.Delete(Path + zfile);
            File.Delete(Path + zfile + ".txt");
        }
        
        public static void CreateZip(string path, string filename)
        {
            var fsOut = File.Create(path + filename + ".zip");
            var zipStream = new ZipOutputStream(fsOut);

            zipStream.SetLevel(9); //0-9, 9 being the highest level 

            var fi = new FileInfo(path + filename);

            var entryName = filename; // Makes the name in zip based on the folder
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
            using (var streamReader = File.OpenRead(path + filename))
            {
                StreamUtils.Copy(streamReader, zipStream, buffer);
            }

            zipStream.IsStreamOwner = true; // Makes the Close also Close the underlying stream
            zipStream.Close();
        }

        public static int GetColNum(SqlConnection connection2, string s)
        {
            var noApos = s;
            noApos = noApos.Replace("'", "''");
            var columnQuery = "SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + noApos + "'";
            using (var command = connection2.CreateCommand())
            {
                command.CommandTimeout = int.MaxValue;
                command.CommandText = columnQuery;

                using (var reader = command.ExecuteReader())
                {
                    reader.Read();
                    var columnNum = reader.GetInt32(0);
                    return columnNum;
                }
            }
        }

        public static string ReadPassword()
        {
            var password = "";
            var info = Console.ReadKey(true);
            while (info.Key != ConsoleKey.Enter)
            {
                if (info.Key != ConsoleKey.Backspace)
                {
                    Console.Write("*");
                    password += info.KeyChar;
                }
                else if (info.Key == ConsoleKey.Backspace)
                {
                    if (!string.IsNullOrEmpty(password))
                    {
                        // remove one character from the list of password characters
                        password = password.Substring(0, password.Length - 1);
                        // get the location of the cursor
                        var pos = Console.CursorLeft;
                        // move the cursor to the left by one character
                        Console.SetCursorPosition(pos - 1, Console.CursorTop);
                        // replace it with space
                        Console.Write(" ");
                        // move the cursor to the left by one character again
                        Console.SetCursorPosition(pos - 1, Console.CursorTop);
                    }
                }
                info = Console.ReadKey(true);
            }
            // add a new line because user pressed enter at the end of their password
            Console.WriteLine();
            return password;
        }

        public static void DoBcp(string path, string s, string query2, string database, string server, int numCol, SqlConnection connection2)
        {
            var sqlServer = "-S " + server;

            if (numCol > 1)
            {
                var colList = new string[2];

                var noApos = s;
                noApos = noApos.Replace("'", "''");
                var columnQuery = "select column_name from information_schema.columns where table_name = '"+ noApos + "' and ordinal_position <= 2";
                using (var command = connection2.CreateCommand())
                {
                    command.CommandTimeout = int.MaxValue;
                    command.CommandText = columnQuery;

                    using (var reader = command.ExecuteReader())
                    {
                        int n = 0;
                       while(reader.Read())
                        { 
                            colList[n] = reader["column_name"].ToString();
                            n++;
                        }
                    }
                }

                query2 = query2.Replace("COLUMNS", colList[0] + "," + colList[1]);
            }

            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Thread #" + Thread.CurrentThread.ManagedThreadId + " finished headers on  " + s);

            var replace = "[" + s + "]";
            
            query2 = query2.Replace("TABLENAME", replace);
            query2 = query2.Replace("DATABASE", database);
            string bcpargs = $"\"{query2}\" queryout \"{path}{s}.txt\" -c -t | -U sa -P liamcow {sqlServer}";
            var bcproc = new System.Diagnostics.Process
            {
                StartInfo =
                {
                    FileName = "bcp",
                    Arguments = bcpargs,
                    UseShellExecute = false,
                    RedirectStandardOutput = true
                }
            };
            bcproc.Start();
            bcproc.OutputDataReceived += null;
            bcproc.BeginOutputReadLine();
            bcproc.WaitForExit();

            var proc = new System.Diagnostics.Process
            {
                StartInfo =
                {
                    FileName = "cmd",
                    Arguments = "/C type \""+ path + s + ".txt\" >> \"" + path + s +"\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true
                }
            };
            proc.Start();
            proc.OutputDataReceived += null;
            proc.BeginOutputReadLine();
            proc.WaitForExit();

            CreateZip(path, s);
            File.Delete(path + s);
            File.Delete(path + s + ".txt");


            // ProcessCmd("bcp",
            //    $"\"{query2}\" queryout \"{Path}{s}.txt\" -c -t | -U sa -P liamcow {sqlServer}", "0");

            // ProcessCmd("type", "\""+ Path + s + ".txt\" >> \"" + Path + s +".psv\"", s.ToString());

            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Thread #" + Thread.CurrentThread.ManagedThreadId + " finished with " + s);
        }
    }

    
}

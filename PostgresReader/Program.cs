using System;
using System.Data;
using System.IO;
using Npgsql;

namespace PostgresReader
{

    /// <summary> 
    /// PostgresReader version 1.0   by Elrey Velicaria 01/2023 
    /// A simple command line utility to pull data from an Oracle Database.
    /// 
    /// Exit Code 0: Means empty result set. 
    /// Exit Code 1: Means with results. 
    /// Exit Code -1: Means error.  See error.log file created. 
    /// 
    /// </summary> 
    class Program
    {

        static string GlobalError = "";

        static int Main(string[] args)
        {
            int ret = -1;

            string delimiter = "\t";

            if (args.Length > 2 && args[2].ToLower().Contains(".csv"))
                delimiter = ",";

            // html prefix and suffixes, activates only on html. 
            string tr1 = string.Empty;
            string tr2 = string.Empty;
            string th1 = string.Empty;
            string th2 = string.Empty;
            string td1 = string.Empty;
            string td2 = string.Empty;
            string thead1 = string.Empty;
            string thead2 = string.Empty;
            string tbody1 = string.Empty;
            string tbody2 = string.Empty;
            string table1 = string.Empty;
            string table2 = string.Empty;


            string ReportTitle = string.Empty;
            string ReportTitleSize = string.Empty;

            if (args.Length > 2 && args[2].ToLower().Contains(".htm"))
            {
                delimiter = "";
                tr1 = "<tr>";
                tr2 = "</tr>";
                th1 = "<th>";
                th2 = "</th>";
                td1 = "<td>";
                td2 = "</td>";
                thead1 = "<thead>";
                thead2 = "</thead>";
                tbody1 = "<tbody>";
                tbody2 = "</tbody>";
                table1 = "<table border=1>";
                table2 = "</table>";

                // get html banner tile from the out filename if it starts with H_  or H1_ , H2_  , etc for diff font size. 

                string n = Path.GetFileName(args[2]);

                if (n.StartsWith("H_"))
                {
                    ReportTitle = n.Replace("H_", "").Replace("_", " ").Replace(".html", "").Replace(".htm", "");
                    ReportTitleSize = "h2";
                }
                if (n.StartsWith("H1_") || n.StartsWith("H2_") || n.StartsWith("H3_") || n.StartsWith("H4_") || n.StartsWith("H5_"))
                {
                    ReportTitle = n.Replace("H1_", "").Replace("H2_", "").Replace("H3_", "").Replace("H4_", "").Replace("H5_", "").Replace("_", " ").Replace(".html", "").Replace(".htm", "");
                    ReportTitleSize = n.Substring(0, 2).ToLower();
                }

            }


            try
            {


                string oradb = args[0];

                if (File.Exists(args[0]))
                    oradb = File.ReadAllText(args[0]);

                log("Connection: " + oradb.Leftmost(100) + "...");

                using (NpgsqlConnection conn = new NpgsqlConnection(oradb))
                {
                    conn.Open();
                    NpgsqlCommand cmd = new NpgsqlCommand();
                    cmd.Connection = conn;

                    if (File.Exists(args[1]))
                    {
                        cmd.CommandText = File.ReadAllText(args[1]);

                    }
                    else
                        cmd.CommandText = args[1];

                    log("Executing sql: " + args[1] + "...");

                    cmd.CommandType = CommandType.Text;

                    using (NpgsqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {

                            // get the header row string. 
                            string f = string.Empty;

                            f = thead1 + tr1;
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                if (i == 0)
                                    f = f + th1 + reader.GetName(i) + th2;
                                else
                                    f = f + delimiter + th1 + reader.GetName(i) + th2;

                            }

                            f = f + tr2 + thead2;

                            if (args.Length > 2)
                            {

                                // Output to a file version. 

                                string l = string.Empty;



                                using (System.IO.StreamWriter file = new System.IO.StreamWriter(args[2]))
                                {

                                    if (ReportTitle != "")
                                        file.WriteLine("<div><" + ReportTitleSize + ">" + ReportTitle + "</" + ReportTitleSize + "></div>");
                                    if (table1 != "")
                                        file.WriteLine(table1);

                                    file.WriteLine(f); //header 

                                    if (tbody1 != "")
                                        file.WriteLine(tbody1);

                                    while (reader.Read())
                                    {

                                        l = tr1;

                                        for (int i = 0; i < reader.FieldCount; i++)
                                        {

                                            if (i == 0)
                                                l = l + td1 + GetFieldAt(reader, i, delimiter) + td2;
                                            else
                                                l = l + delimiter + td1 + GetFieldAt(reader, i, delimiter) + td2;
                                        }

                                        l = l + tr2;

                                        file.WriteLine(l);

                                    }

                                    if (tbody2 != "")
                                        file.WriteLine(tbody2);
                                    if (table2 != "")
                                        file.WriteLine(table2);

                                }
                            }
                            else
                            {
                                // Output to console version is always tab delimited 

                                Console.WriteLine(f);

                                while (reader.Read())
                                {
                                    string l = string.Empty;

                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        //Console.WriteLine(i); 

                                        if (i == 0)
                                            l = l + GetFieldAt(reader, i, delimiter);
                                        else
                                            l = l + "\t" + GetFieldAt(reader, i, delimiter);
                                    }


                                    Console.WriteLine(l);
                                }
                            }

                            ret = 1;
                        }
                        else
                        {
                            log("No rows found.");

                            ret = 0;
                        }
                        reader.Close();

                    }
                }


            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                help();
                log("Error: " + e.Message);

                ret = -1;
            }

            if (GlobalError != "")
                log("Field errrors: " + GlobalError);

            return ret;


        }

        private static void help()
        {

            Console.WriteLine("---------------------------------------------------------------------------------------------------");
            Console.WriteLine("PostgresReader - A simple command line utility to pull data from an Oracle Database. ");
            Console.WriteLine("Version {0}              Author: Elrey R. Velicaria (10/1/2020)    ", System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
            Console.WriteLine("Syntax: PostgresReader.exe <ConnectionInfo> <SQLQuery> [OutputFile]");
            Console.WriteLine("Examples:");
            Console.WriteLine("  Console output all params as  inline  : PostgresReader.exe \"conn string\" \"select * from.. \"");
            Console.WriteLine("  Connect string in file, inline query  : PostgresReader.exe ConnStr.txt \"select * from product \"");
            Console.WriteLine("  Console output with params in files   : PostgresReader.exe ConnStr.txt Query.sql");
            Console.WriteLine("  Tab delimited  if out file is .txt    : PostgresReader.exe ConnStr.txt Query.sql ouput.txt");
            Console.WriteLine("  Comma delimited if out file is .csv   : PostgresReader.exe ConnStr.txt Query.sql ouput.csv");
            Console.WriteLine("  Html table formatted if  .htm or .html: PostgresReader.exe ConnStr.txt Query.sql ouput.htm");
            Console.WriteLine("  Html table with banner def h2 format  : PostgresReader.exe ConnStr.txt Query.sql H_Table_A.htm");
            Console.WriteLine("  Html with banner size format (H1..H5) : PostgresReader.exe ConnStr.txt Query.sql H1_Table_A.htm");
            Console.WriteLine("Notes:");
            Console.WriteLine("* DOS Exit Codes are:  0 = no rows,  1 = with rows,  -1 = error");
            Console.WriteLine("* Logs to log.txt in a sub-folder named OracleExecLog , if it exists.");
            Console.WriteLine("* Oracle client not required. It uses Oracle.ManagedDataAccess.dll as 'thin' driver library. Enjoy!");
            Console.WriteLine("---------------------------------------------------------------------------------------------------");
        }

        // Simplest logger. 
        private static void log(string s)
        {
            try
            {
                File.AppendAllText(".\\OracleExecLog\\log.txt", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ") + "OER: " + s + Environment.NewLine);
            }
            catch
            {
            }
        }

        private static string GetFieldAt(NpgsqlDataReader dr, int pos, string delimiter)
        {
            string s = string.Empty;
            string f = dr.GetName(pos);
            Type t = dr.GetFieldType(pos);
            try
            {

                if (t.Name == "DateTime")
                {
                    s = string.Format("{0:yyyy'-'MM'-'dd' 'HH':'mm':'ss}", dr.GetDateTime(pos)).Replace("00:00:00", "").Trim();
                }
                else if (t.Name.StartsWith("Int"))
                    s = string.Format("{0}", dr.GetDecimal(pos));
                else if (t.Name == "Double" || t.Name == "Single")
                    s = string.Format("{0}", dr.GetDouble(pos));
                else if (t.Name == "Decimal")
                    s = string.Format("{0}", dr.GetDecimal(pos));
                else if (t.Name == "String")
                {   // csv will have double quotes on strings. 
                    if (delimiter == ",")
                        s = s = string.Format("\"{0}\"", dr.GetString(pos));
                    else
                        s = s = string.Format("{0}", dr.GetString(pos));
                }
                else
                {
                    string msg = f + ": Unsupported type " + t.Name;
                    if (GlobalError == "")
                        GlobalError = msg;
                    else
                    {
                        if (!GlobalError.Contains(msg)) GlobalError += ", " + msg;
                    }
                    s = "***"; // unsupported types 
                }

                if (s == "null" || s == "\"\"" || s == "\"null\"") s = "";
            }
            catch (Exception e)
            {
                string msg = f + ":" + e.Message;
                if (GlobalError == "")
                    GlobalError = msg;
                else
                {
                    if (!GlobalError.Contains(msg)) GlobalError += ", " + msg;
                }

                s = "";
            }
            return s;
        }
    }


    // Functions for convenience. 
    public static class StringExtensions
    {
        public static string Leftmost(this string str, int length)
        {
            return str.Substring(0, Math.Min(length, str.Length));
        }

        public static string Rightmost(this string str, int length)
        {
            return str.Substring(str.Length - Math.Min(length, str.Length));
        }
    }

}


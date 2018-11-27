using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Data;

using MySql.Data;
using MySql.Data.MySqlClient;


namespace imapLibrary
{
    public class mySqlConnector
    {

        //private OleDbConnection con;
        private MySql.Data.MySqlClient.MySqlConnection con;

        public static string ConnectionString = "server=195.34.150.12;port=3306;uid=mail;pwd=20qsecofr16;database=mail;";


        private int timeout = 300;


        /*
         * type:
         * 0 = intern
         * 1 = survey3
         */

        public mySqlConnector()
        {
            // ConnectionString = "Data Source=195.34.150.12;Port=3306;User ID=mail;Password=20qsecofr16;database=mail;";

            try
            {
                con = new MySql.Data.MySqlClient.MySqlConnection(ConnectionString);
                con.Open();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }



        public MySqlDataReader ExecuteReader(string strSql)
        {
            if (con == null) return null;

            MySqlCommand cmd = new MySqlCommand(strSql, con);
            MySqlDataReader reader = null;

            try
            {
                cmd.CommandTimeout = timeout;
                reader = cmd.ExecuteReader();
            }
            catch (MySqlException ex)
            {
                Debug.WriteLine("Fehler @executeReader: " + ex.Message + " || " + strSql);
                cmd.Dispose();
                return null;
            }

            return reader;
        } //ENDE ExecuteReader





        //Rückgabewert null = Fehler!
        public object executeScalar(string strSql, object[,] arrParameters = null)
        {
            if (con == null) return null;

            try
            {
                MySqlCommand cmd = new MySqlCommand(strSql, con);

                //Parameters
                if (arrParameters != null)
                {
                    if (arrParameters.GetLength(1) > 1)
                    {
                        for (int i = 0; i < arrParameters.GetLength(0); i++)
                        {
                            cmd.Parameters.AddWithValue(arrParameters[i, 0].ToString(), arrParameters[i, 1]);
                        }
                    }
                }

                cmd.CommandTimeout = timeout;
                object oResult = cmd.ExecuteScalar();
                cmd.Dispose();

                return oResult;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Fehler @executeScalar: " + ex.Message + " || " + strSql);
                return null;
            }

        } //ENDE executeScalar





        //Rückgabewert false = Fehler!
        public bool executeNonQuery(string sqlStr, object[,] arrParameters = null)
        {
            if (con == null) return false;


            try
            {
                MySqlCommand cmd = new MySqlCommand(sqlStr, con);

                //Parameters
                if (arrParameters != null)
                {
                    if (arrParameters.GetLength(1) > 1)
                    {
                        for (int i = 0; i < arrParameters.GetLength(0); i++)
                        {
                            cmd.Parameters.AddWithValue(arrParameters[i, 0].ToString(), arrParameters[i, 1]);
                        }
                    }
                }

                cmd.CommandTimeout = timeout;
                cmd.ExecuteNonQuery();
                cmd.Dispose();

                //System.Diagnostics.Debug.WriteLine("executeNonQuery ausgeführt - " + sqlStr);

                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Fehler @executeNonQuery: " + ex.Message + " || " + sqlStr);
                return false;
            }

        } //ENDE executeNonQuery





        public DataTable MySqlToDataTable(string sqlStr)
        {
            DataTable dt = new DataTable();

            using (var da = new MySqlDataAdapter(sqlStr, con))
            {
                var table = new DataTable();
                da.Fill(dt);
            }

            return dt;

        } //ENDE SqlToDataTable




        /*
         * erstellt einen MD5-Hash aus einem String
         * wird benötigt fürs Speichern der Mailpasswörter in der MySql-Datenbank
         */

        public string CalculateMD5Hash(string input)
        {
            // step 1, calculate MD5 hash from input
            System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create();

            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }

            return sb.ToString();
        } //ENDE CalculateMD5Hash




        public void Close()
        {
            con.Close();
        } //ENDE Close

    }
}

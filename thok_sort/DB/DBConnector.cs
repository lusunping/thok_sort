using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.IO;
using System.Data;
using MySql.Data;
using System.Web;
using System.Windows.Forms;

namespace DBAssist
{
    class DBConnector
    {
        private MySqlConnection connection;
        public MySqlConnection Connection
        {
            get { return connection; }
            set { connection = value; }
        }
        private string server;
        public string Server 
        { 
            get{return server;}
            set{server = value;}
        }
        private string database;
        public string Database
        {
            get { return database; }
            set { database = value; }
        }
        private string uid;
        public string Uid
        {
            get { return uid; }
            set { uid = value; }
        }
        private string password;
        public string Password
        {
            get { return password; }
            set { password = value; }
        }

        public DBConnector()
        {
            Initialize();
        }

        private void Initialize()
        {
            server = "localhost";
            database = "dzdb";
            uid = "DZDB";
            password = "password";
            string connectionString;
           // connectionString = "SERVER=" + server + ";" + "DATABASE=" + 
		    //database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";";
            connectionString = "User Id = DZDB;Password = password;Host = localhost;Database = dzdb;";
            connection = new MySqlConnection(connectionString);
            
        }

        public bool OpenConnection()
        {
            try
            {
                if (connection.State == ConnectionState.Open) return true;
                    else connection.Open();
                return true;
            }
            catch (MySqlException ex)
            {

                switch (ex.Number)
                {
                    case 0:
                         MessageBox.Show("Cannot connect to server.  Contact administrator");
                        break;

                    case 1045:
                        MessageBox.Show("Invalid username/password, please try again");
                        break;
                }
                return false;
            }
        }

        public bool CloseConnection()
        {
            try
            {
                connection.Close();
                return true;
            }
            catch (MySqlException ex)
            {
                MessageBox.Show(ex.Message);
                return false;
            }
        }

        public int Insert(string sql)
        {
            //string query = "INSERT INTO tableinfo (name, age) VALUES('John Smith', '33')";
            int i = 0;
            //open connection
            if (this.OpenConnection() == true)
            {
                //create command and assign the query and connection from the constructor
                MySqlCommand cmd = new MySqlCommand(sql, Connection);

                //Execute command
                i = cmd.ExecuteNonQuery();
                
                //close connection
                this.CloseConnection();
                
            }
            return i;
        }

        public void Update(string sql)
        {
            //string query = "UPDATE tableinfo SET name='Joe', age='22' WHERE name='John Smith'";

            //Open connection
            if (this.OpenConnection() == true)
            {
                //create mysql command
                MySqlCommand cmd = new MySqlCommand();
                //Assign the query using CommandText
                cmd.CommandText = sql;
                //Assign the connection using Connection
                cmd.Connection = connection;

                //Execute query
                cmd.ExecuteNonQuery();

                //close connection
                this.CloseConnection();
            }
        }

        public void Delete(string sql)
        {
            //string query = "DELETE FROM tableinfo WHERE name='John Smith'";

            if (this.OpenConnection() == true)
            {
                MySqlCommand cmd = new MySqlCommand(sql, connection);
                cmd.ExecuteNonQuery();
                this.CloseConnection();
            }
        }

        public List<string> Select(string sql)
        {

            //Create a list to store the result
            List<string> list = new List<string>();
            
            //Open connection
            if (this.OpenConnection() == true)
            {
                //Create Command
                MySqlCommand cmd = new MySqlCommand(sql, connection);
                //Create a data reader and Execute the command
                MySqlDataReader dataReader = cmd.ExecuteReader();

                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        list.Add(dataReader[i].ToString());
                    }

                } 
                //close Data Reader
                dataReader.Close();

                //close Connection
                this.CloseConnection();

                //return list to be displayed
                return list;
            }
            else
            {
                return list;
            }
        }

        public int Count()
        {
            string query = "SELECT Count(*) FROM tableinfo";
            int Count = -1;

            //Open Connection
            if (this.OpenConnection() == true)
            {
                //Create Mysql Command
                MySqlCommand cmd = new MySqlCommand(query, connection);

                //ExecuteScalar will return one value
                Count = int.Parse(cmd.ExecuteScalar() + "");

                //close Connection
                this.CloseConnection();

                return Count;
            }
            else
            {
                return Count;
            }
        }

        public static void CreateCSVfile(DataTable dtable, string strFilePath)
        {
            StreamWriter sw = new StreamWriter(strFilePath, false);
            int icolcount = dtable.Columns.Count;
            foreach (DataRow drow in dtable.Rows)
            {
            for (int i = 0; i < icolcount; i++)
            {
                if (!Convert.IsDBNull(drow[i]))
                {
                sw.Write(drow[i].ToString());
                }
                if (i < icolcount - 1)
                {
                sw.Write(",");
                }
            }
            sw.Write(sw.NewLine);
            }
            sw.Close();
            sw.Dispose();
        }


        public  void ImportMySQL()
        {
            DataTable orderDetail = new DataTable("ItemDetail");
            DataColumn c = new DataColumn();        // always
            orderDetail.Columns.Add(new DataColumn("ID", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("name", Type.GetType("System.Char")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type1_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type2_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type3_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type4_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type5_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type6_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type7_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type8_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type9_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type10_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type11_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type12_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type13_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type14_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type15_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type16_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type17_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type18_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type19_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("cigarette_type20_num", Type.GetType("System.Int32")));
            orderDetail.Columns.Add(new DataColumn("toc", Type.GetType("System.DateTime")));
            //orderDetail.Columns.Add(new DataColumn("total", Type.GetType("System.Decimal")));
            //orderDetail.Columns["total"].Expression = "value/(length*breadth)";
            
            //Adding dummy entries
            DataRow dr = orderDetail.NewRow();
            dr["id"] = 1;
            dr["name"] = "s";
            dr["cigarette_type1_num"] = 1;
            dr["cigarette_type2_num"] = 2;
            dr["cigarette_type3_num"] = 3;
            dr["cigarette_type4_num"] = 4;
            dr["cigarette_type5_num"] = 3;
            dr["cigarette_type6_num"] = 2;
            dr["cigarette_type7_num"] = 2;
            dr["cigarette_type8_num"] = 2;
            dr["cigarette_type9_num"] = 2;
            dr["cigarette_type10_num"] =2;
            dr["cigarette_type11_num"] =2;
            dr["cigarette_type12_num"] =2;
            dr["cigarette_type13_num"] =2;
            dr["cigarette_type14_num"] =2;
            dr["cigarette_type15_num"] =2;
            dr["cigarette_type16_num"] =2;
            dr["cigarette_type17_num"] =2;
            dr["cigarette_type18_num"] =2;
            dr["cigarette_type19_num"] =2;
            dr["cigarette_type20_num"] =2;
            dr["toc"] = DateTime.Now;
            orderDetail.Rows.Add(dr);
            //Adding dummy entries

            //string connectMySQL = "Server=localhost;Database=mysql;Uid=DZDB;Pwd=password;";
            string connectMySQL = "User Id = DZDB;Password = password;Host = localhost;Database = dzdb;";
           // MySqlConnection mySqlCon = new MySqlConnection("User Id = DZDB;Password = password;Host = localhost;Database = mysql;");
            string strFile = "/MySQL" + DateTime.Now.Ticks.ToString() + ".csv";
 
            //Create directory if not exist... Make sure directory has required rights..
            string Paths = "C:/Users/lusp117021/Documents/Visual Studio 2010/Projects/cigaretee_sort/cigaretee_sort/TempFolder/";
            if (!Directory.Exists(Paths))
                Directory.CreateDirectory(Paths);
            //if (!Directory.Exists(System.Web.HttpContext.Current.Server.MapPath("~/TempFolder/")))
            //    Directory.CreateDirectory(System.Web.HttpContext.Current.Server.MapPath("~/TempFolder/"));
            
            //If file does not exist then create it and right data into it..
            if (!File.Exists(Paths+strFile))
            {
                FileStream fs = new FileStream(Paths + strFile, FileMode.Create, FileAccess.Write);
                fs.Close();
                fs.Dispose();
            }
 
            //Generate csv file from where data read
            CreateCSVfile(orderDetail, Paths + strFile);
            using (MySqlConnection cn1 = new MySqlConnection(connectMySQL))
            {
                cn1.Open();
                MySqlBulkLoader bcp1 = new MySqlBulkLoader(cn1);
                bcp1.TableName = "orders"; 
                bcp1.FieldTerminator = ",";
 
                bcp1.LineTerminator = "\r\n";
                bcp1.FileName = Paths + strFile;
                bcp1.NumberOfLinesToSkip = 0;
                bcp1.Load();
 
            //Once data write into db then delete file..
            try
            {
                //File.Delete(Paths + strFile);
            }
            catch (Exception ex)
            {
                string str = ex.Message;
            }
            }
        }
    }
}

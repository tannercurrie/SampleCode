// Tanner Currie (C) 2019
// tannercurrie@yahoo.com
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MultiDBSelectUtility
{
    public partial class Form1 : Form
    {
        private string connectionString = "Data Source=ORB-SRBDB1;Initial Catalog=ORB_SRBSystemdatabase_B;Integrated Security=True";
        private Dictionary<string, CustomerContainer> m_customerList = new Dictionary<string, CustomerContainer>();
        private Dictionary<string, string> m_dbLookUp = new Dictionary<string, string>();

        private int m_rowCount = 0;

        public Form1()
        {
            InitializeComponent();
            QueryTextBox.Text = "SELECT Email, FirstName, LastName FROM [{0}].[PlatformSchema].[Users] where[IsActive] = '1'";
        }

        private async void GenerateButton_Click(object sender, EventArgs e)
        {
            SqlConnection conn = null;
            try
            {
                conn = new SqlConnection(connectionString);
                await conn.OpenAsync();

                string query = "SELECT Id, DBServer, CustomerName, CustomerNumber FROM [ORB_SRBSystemdatabase_A].[dbo].[Subscriptions]";

                SqlCommand command = new SqlCommand(query, conn);

                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        CustomerContainer dbData = null;

                        string customerName = Convert.ToString(reader["CustomerName"]);
                        string dbServer = Convert.ToString(reader["DBServer"]);

                        if (dbServer.ToUpper().Contains("FACON") == false && dbServer.Contains(".bzasprod.internal") == true)
                        {
                            dbServer = dbServer.Remove(10).ToUpper();
                        }
                        else
                        {
                            continue;
                        }

                        if (m_customerList.ContainsKey(customerName) == false)
                        {
                            dbData = new CustomerContainer(dbServer, Convert.ToString(reader["Id"]),
                                Convert.ToString(reader["CustomerNumber"]), Convert.ToString(reader["CustomerName"]));
                            m_customerList.Add(customerName, dbData);
                        }
                    }
                }

                conn.Close();

                MessageBox.Show(m_customerList.Count().ToString());

                WriteToCsvFile();

            }
            catch (Exception ex)
            {
                MessageBox.Show(String.Concat(ex.Message, Environment.NewLine, ex.StackTrace), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// Create a CSV file from the populated CustomerContainer lookup table
        /// </summary>
        public void WriteToCsvFile()
        {
            try
            {
                if (m_customerList.Count == 0)
                {
                    MessageBox.Show("zero convert list");
                    return;
                }

                var csv = new StringBuilder();

                foreach (CustomerContainer cust in m_customerList.Values)
                {
                    if (cust.User != null && cust.User.Count > 0)
                    {
                        foreach (UserInstance user in cust.User)
                        {
                            var newLine = string.Format("{0},{1},{2},{3},{4},{5},{6}", cust.CustomerName, cust.CustomerNumber, cust.SubscritionId, cust.Database,
                                user.FirstName, user.LastName, user.Email);
                            csv.AppendLine(newLine);
                        }
                    }
                    else
                    {
                        var newLine = string.Format("{0},{1},{2},{3}", cust.CustomerName, cust.CustomerNumber, cust.SubscritionId, cust.Database);
                        csv.AppendLine(newLine);
                    }
                }

                File.WriteAllText("CustomerList.csv", csv.ToString());
            }
            catch (Exception ex)
            {
                MessageBox.Show(String.Concat(ex.Message, Environment.NewLine, ex.StackTrace), "Error", MessageBoxButtons.OK, MessageBoxIcon.Hand);
            }
        }

        /// <summary>
        /// Returns the number of subscriptions in the system
        /// </summary>
        private async void RowCountButton_Click(object sender, EventArgs e)
        {
            SqlConnection conn = null;
            try
            {
                conn = new SqlConnection(connectionString);
                await conn.OpenAsync();

                string query = "SELECT Count(*) FROM [ORB_SRBSystemdatabase_B].[dbo].[Subscriptions]";
                SqlCommand command = new SqlCommand(query, conn);
                m_rowCount = Convert.ToInt32(await command.ExecuteScalarAsync());
                RowCountLabel.Text = String.Format("Row Count: {0}", m_rowCount);
                conn.Close();
            }
            catch (Exception ex)
            {
                MessageBox.Show(String.Concat(ex.Message, Environment.NewLine, ex.StackTrace), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// Assemble the User table and generate the CSV file
        /// </summary>
        private async void UserTableButton_Click(object sender, EventArgs e)
        {
            string connectionStr = "Data Source={0};Initial Catalog={1};Integrated Security=True";
            SqlConnection conn = null;
            try
            {
                foreach (CustomerContainer customer in m_customerList.Values)
                {
                    string dbServer = customer.Database;
                    string dbName = string.Empty;

                    m_dbLookUp.TryGetValue(customer.SubscritionId, out dbName);

                    if (String.IsNullOrEmpty(dbName) == true)
                    {
                        continue;
                    }

                    // query the unique customer database server and name
                    string connection = String.Format(connectionStr, dbServer, dbName);

                    conn = new SqlConnection(connection);
                    await conn.OpenAsync();

                    // find all the active users on this subscription
                    string queryString = "SELECT Email, FirstName, LastName FROM [{0}].[PlatformSchema].[Users] where[IsActive] = '1'";
                    string query = String.Format(queryString, dbName);

                    SqlCommand command = new SqlCommand(query, conn);

                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        List<UserInstance> userList = new List<UserInstance>();
                        while (reader.Read())
                        {
                            userList.Add(new UserInstance(Convert.ToString(reader["Email"]), Convert.ToString(reader["FirstName"]),
                                Convert.ToString(reader["LastName"])));
                        }

                        customer.User = userList;
                    }

                    conn.Close();
                }

                WriteToCsvFile();
            }
            catch (Exception ex)
            {
                MessageBox.Show(String.Concat(ex.Message, Environment.NewLine, ex.StackTrace), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// Execute a SQL query from the QueryTextBox
        /// </summary>
        /// <remarks>probe test method to be used with this active db connection instance</remarks>
        private async void ExecuteQueryButton_Click(object sender, EventArgs e)
        {
            string dbServer = DBServerComboBox.SelectedItem.ToString();
            string query = QueryTextBox.Text;
            string dbName = DbNameTextBox.Text;
            string connectionStr = "Data Source={0};Initial Catalog={1};Integrated Security=True";
            SqlConnection conn = null;

            try
            {
                string connection = String.Format(connectionStr, dbServer, dbName);

                conn = new SqlConnection(connection);
                await conn.OpenAsync();

                SqlCommand command = new SqlCommand(query, conn);

                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    List<UserInstance> userList = new List<UserInstance>();
                    while (reader.Read())
                    {
                        ResultTextBox.Text = String.Concat(ResultTextBox.Text, Environment.NewLine, reader.GetString(0), "  :  ", reader.GetString(1), "  :  ", reader.GetString(2));
                    }
                }

                conn.Close();
            }
            catch (Exception ex)
            {
                MessageBox.Show(String.Concat(ex.Message, Environment.NewLine, ex.StackTrace), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// Find each unique customer DB name from the ApplicationSubscription table
        /// and Populate a dictionary mapping with the customer Subscription ID
        /// </summary>
        private async void GetDatabaseNamesButton_Click(object sender, EventArgs e)
        {
            SqlConnection conn = null;

            if (m_dbLookUp.Count > 1)
            {
                foreach(string dbKey in m_dbLookUp.Keys)
                {
                    ResultTextBox.Text = String.Concat(ResultTextBox.Text, Environment.NewLine, "Subscr ID: ", dbKey, "DB: ", m_dbLookUp[dbKey]);
                }
            }
            else
            {
                try
                {
                    conn = new SqlConnection(connectionString);
                    await conn.OpenAsync();

                    string query = "SELECT SubscriptionId, DBName FROM [ORB_SRBSystemdatabase_B].[dbo].[ApplicationSubscription] Order By DBName";

                    SqlCommand command = new SqlCommand(query, conn);

                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {

                        while (reader.Read())
                        {
                            string subscriptionId = Convert.ToString(reader["SubscriptionId"]);
                            string dbName = Convert.ToString(reader["DBName"]);

                            if (dbName.Contains("_fa") == true)
                            {
                                continue;
                            }
                            else if (m_dbLookUp.ContainsKey(subscriptionId) == false)
                            {
                                m_dbLookUp.Add(subscriptionId, dbName);
                                ResultTextBox.Text = String.Concat(ResultTextBox.Text, Environment.NewLine, "Subscr ID: ", subscriptionId, "DB: ", dbName);
                            }
                        }
                    }

                    conn.Close();

                    MessageBox.Show(String.Format("Complete: {0} Rows", m_dbLookUp.Count));


                }
                catch (Exception ex)
                {
                    MessageBox.Show(String.Concat(ex.Message, Environment.NewLine, ex.StackTrace), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }
            }
        }

        private void ClearResultsButton_Click(object sender, EventArgs e)
        {
            ResultTextBox.Text = String.Empty;
        }

        private void FormatButton_Click(object sender, EventArgs e)
        {
            QueryTextBox.Text = String.Format(QueryTextBox.Text, DbNameTextBox.Text);
        }
    }

    /// <summary>
    /// Data container class for a Customer instance
    /// </summary>
    public class CustomerContainer
        {
            private List<UserInstance> m_user = new List<UserInstance>();

            public CustomerContainer(string database, string subscriotionId, string customerNumber, string customerName)
            {
                Database = database;
                SubscritionId = subscriotionId;
                CustomerNumber = customerNumber;
                CustomerName = customerName;
            }

            /// <summary>
            /// Customer Database
            /// </summary>
            public string Database
            {
                get;
                set;
            }

            /// <summary>
            /// Subscription ID         
            /// </summary>
            public string SubscritionId
            {
                get;
                set;
            }
            
            /// <summary>
            /// Customer Number
            /// </summary>
            public string CustomerNumber
            {
                get;
                set;
            }

            /// <summary>
            /// Customer Name
            /// </summary>
            public string CustomerName
            {
                get;
                set;
            }

            /// <summary>
            /// List of each unique user that belongs to this customer
            /// </summary>
            public List<UserInstance> User
            {
                get
                {
                    return m_user;
                }
                set
                {
                    m_user = value;
                }
            }
        }

    /// <summary>
    /// User instance container class
    /// </summary>
    public class UserInstance
    {
        public UserInstance()
        { }

        public UserInstance(string email, string firstName, string lastName)
        {
            Email = email;
            FirstName = firstName;
            LastName = lastName;
        }

        public string Email
        {
            get;
            set;
        }

        public string FirstName
        {
            get;
            set;
        }

        public string LastName
        {
            get;
            set;
        }
    }
}

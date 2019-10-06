using System;
using Newtonsoft.Json;
using System.Data;
using System.Net.Http;
using System.Text;
using System.Linq;
using System.IO;
using MySql.Data.MySqlClient; // MySql Adapter

// Install System.Data.DataSetExtensions to Access AsEnumerable()

namespace integrationExampleUsingCsharp
{
  class Program
  {
    private const string RequestUri = ""; // nana enpoint URL e.g (https://nana.sa/api/sync_store_products_by_key)
    private static string getBranchQuery = ""; // query for branches and corresponding tokens
    private static string StoreId = ""; // local branch Id
    private static string Token = ""; // corresponding token
    private static string getDataQuery = ""; // query records either changed products or daily sales

    static void Main(string[] args)
    {
        string connStr = "server=localhost;database=dotnet;uid=root;pwd=root"; // connection string to the database
        using (MySqlConnection conn = new MySqlConnection(connStr)) // initiate connection
        {
            try
            {
              Console.WriteLine("Connecting To MySql ...");
              using (MySqlDataAdapter da = new MySqlDataAdapter()) // create new instance for the mysql adapter
              {
                using (DataTable dt1 = new DataTable()) // initiate new data table
                {
                  using (MySqlCommand sqlCommand1 = conn.CreateCommand()) // prepare new sql command
                  {
                    getBranchQuery = ""; // write query text
                    // query example (select name, token from stores)
                    sqlCommand1.CommandType = CommandType.Text; // define command type
                    sqlCommand1.CommandText = getBranchQuery; // add command text
                    da.SelectCommand = sqlCommand1; // execute command
                    da.Fill(dt1); // fill record to the data table declared above
                    sqlCommand1.Dispose();
                    da.Dispose();
                    foreach (DataRow row in dt1.Rows) // foreach branch get either changed records or daily sales
                    {
                      Token = row["token"].ToString(); // assign store token
                      StoreId = row["name"].ToString(); // assign store id
                      using (DataTable dt = new DataTable()) // initiate new data table
                      {
                        using (MySqlCommand sqlCommand = conn.CreateCommand()) // prepare new sql command
                        {
                          getDataQuery = ""; // write query text
                          // select name, phone, store_id from users where store_id='" + StoreId + "'
                          sqlCommand.CommandType = CommandType.Text; // define command type
                          sqlCommand.CommandText = getDataQuery; // add command text
                          da.SelectCommand = sqlCommand; // execute command
                          da.Fill(dt); // fill record to the data table declared above
                          sqlCommand.Dispose();
                          da.Dispose();
                          Console.WriteLine("Preparing Batches");
                          var tr = dt.Rows.Count; // get total records
                          tr = (int)Math.Ceiling((Double)tr / 10); // divide batches 10 per each
                          var skipCount = 0; // prepare skipping records
                          for (int i = 0; i < tr; i++) // prepare skipping records
                          {
                            Console.WriteLine("Sending " + (i + 1) + " Batch");
                            var dataBatch = dt.AsEnumerable().Skip(skipCount).Take(10); // take every tr number and skip
                            using (DataTable copyTableData = dataBatch.CopyToDataTable<DataRow>()) // copy batched to new datatable
                            {
                              var jsonResult = JsonConvert.SerializeObject(new {product_arrays = copyTableData}); // serialize copied data to JSON object (product_arrays key for product_sync and barcode_arrays for daily sales)
                              Console.WriteLine(jsonResult);
                              sendRequest(jsonResult); // send request
                              skipCount += 10; // skip the next batch
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        Console.WriteLine("");
        Console.WriteLine("Done ...");
    }
    static void sendRequest(String data)
    {
      try
      {
        using (HttpClient client = new HttpClient())
          {
              client.DefaultRequestHeaders.Add("Authorization", Token); // append token got from stores query
              client.DefaultRequestHeaders.Add("User-Agent", "Al-Jazeera");

              using (HttpContent content = new StringContent(data, Encoding.UTF8, "application/json")) // prepare data and headers
              {
                  using (HttpResponseMessage response = client.PostAsync(RequestUri, content).Result)
                  {
                      Console.WriteLine(response.Content.ReadAsStringAsync().Result);
                      if (!File.Exists("Log.txt"))
                      {
                        File.Create("Log.txt");
                      }
                      Console.WriteLine("Batch Sent");
                      File.AppendAllText("Log.txt", "Branch Number " + StoreId + Environment.NewLine);
                      File.AppendAllText("Log.txt", DateTime.Now.ToString() + Environment.NewLine);
                      File.AppendAllText("Log.txt", response.Content.ReadAsStringAsync().Result + Environment.NewLine);
                      File.AppendAllText("Log.txt", "============================================================" + Environment.NewLine);
                  }
              }
          }
      }
      catch (Exception e)
      {
        Console.WriteLine(e.Message);
      }
    }
  }
}

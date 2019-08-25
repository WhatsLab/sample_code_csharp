using System;
using Newtonsoft.Json;
using System.Data;
using System.Net.Http;
using System.Text;
using System.Linq;
using MySql.Data.MySqlClient; // MySql Adapter

// Install System.Data.DataSetExtensions to Access AsEnumerable()

namespace integrationExampleUsingCsharp
{
  class Program
  {
    private const string RequestUri = ""; // Endpoint URL (e.g: https://nana.sa/api/sync_store_products_by_key)
    private const string Token = ""; // JWT Token (Token will be provided for each branch individually)
    private const string query = ""; // Query String (e.g select unit_price as price, item_number as sku, barcode as barcode from products)
    static void Main(string[] args)
    {
        string connStr = "server=localhost;database=database;uid=root;pwd=root"; // Connection String
        using (MySqlConnection conn = new MySqlConnection(connStr))
        {
            try
            {
                Console.WriteLine("Connecting To MySql ...");
                using (MySqlDataAdapter da = new MySqlDataAdapter()) // Generate The MySql Adapter 
                {
                    using (DataTable dt = new DataTable())
                    {
                        using (MySqlCommand sqlCommand = conn.CreateCommand()) // Create MySql Command
                        {
                            sqlCommand.CommandType = CommandType.Text;
                            sqlCommand.CommandText = query;
                            da.SelectCommand = sqlCommand; // Execute MySql Command
                            da.Fill(dt); // Dump Result into DataTable
                            sqlCommand.Dispose();
                            da.Dispose();
                            Console.WriteLine("Preparing Batches");
                            var tr = dt.Rows.Count; // Get Total Rows Count
                            tr = (int)Math.Ceiling((Double)tr / 10); // Divide total by the batches
                            var skipCount = 0;
                            for (int i = 0; i < tr; i++) // Loop through the total table rows
                            {
                                Console.WriteLine("Sending " + (i + 1) + " Batch");
                                var dataBatch = dt.AsEnumerable().Skip(skipCount).Take(10); // Pick 1000 from the total results 
                                using (DataTable copyTableData = dataBatch.CopyToDataTable<DataRow>()) // Copy batched to new datatable
                                {
                                    var jsonResult = JsonConvert.SerializeObject(new {product_arrays = copyTableData}); // Serialize Copied Data To JSON Object (product_arrays key for product_sync and barcode_arrays for daily sales)
                                    sendRequest(jsonResult);
                                    skipCount += 10; // Increase skipCount by 1000 to pick the next batch
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
              client.DefaultRequestHeaders.Add("Authorization", Token); // Add Authorization Header defined on line 16

              using (HttpContent content = new StringContent(data, Encoding.UTF8, "application/json")) // Prepare JSON Payload To Send
              {
                  using (HttpResponseMessage response = client.PostAsync(RequestUri, content).Result) // Send HTTP request and store result in (response)
                  {
                      Console.WriteLine(response.Content.ReadAsStringAsync().Result); // Print Out Response
                      Console.WriteLine("Batch Sent");
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

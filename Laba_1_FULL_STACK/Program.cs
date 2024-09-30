using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Npgsql;
using System.Text.Json;
using System;


namespace Laba_1_FULL_STACK
{
    internal class Program
    {

        static async Task Main(string[] args)
        {
            NpgsqlConnection conn = CreateConnection();
            CreateTable(conn);
            await FetchAndInsertData(conn);
            ReadData(conn);
            conn.Close();
        }

        static NpgsqlConnection CreateConnection()
        {
            var connString = "Host=localhost; Port=5433; Username=postgres; Password=s1a2s3h4a5; Database=postgres";
            NpgsqlConnection conn = new NpgsqlConnection(connString);
            conn.Open();
            return conn;
        }

        static void CreateTable(NpgsqlConnection conn)
        {
            string sql = @"CREATE TABLE IF NOT EXISTS population_data (
                            id SERIAL PRIMARY KEY,
                            id_nation VARCHAR(20),
                            nation VARCHAR(100),
                            id_year INT UNIQUE,
                            year VARCHAR(4),
                            population BIGINT,
                            slug_nation VARCHAR(50))";
            using (var cmd = new NpgsqlCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        static async Task FetchAndInsertData(NpgsqlConnection conn)
        {
            string apiUrl = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";
            using HttpClient client = new HttpClient();
            HttpResponseMessage response = await client.GetAsync(apiUrl);

            if (response.IsSuccessStatusCode)
            {
                string jsonData = await response.Content.ReadAsStringAsync();
                JObject data = JObject.Parse(jsonData);

                foreach (var item in data["data"])
                {
                    string idNation = (string)item["ID Nation"];
                    string nation = (string)item["Nation"];
                    int idYear = (int)item["ID Year"];
                    string year = (string)item["Year"];
                    long population = (long)item["Population"];
                    string slugNation = (string)item["Slug Nation"];
                    InsertData(conn, idNation, nation, idYear, year, population, slugNation);
                }
            }
            else
            {
                Console.WriteLine($"Помилка запиту: {response.StatusCode}");
            }
        }

        static void InsertData(NpgsqlConnection conn, string idNation, string nation, int idYear, string year, long population, string slugNation)
        {
            string sql = @"INSERT INTO population_data (id_nation, nation, id_year, year, population, slug_nation) 
                   VALUES (@IdNation, @Nation, @IdYear, @Year, @Population, @SlugNation) 
                   ON CONFLICT (id_year) DO UPDATE SET 
                       id_nation = EXCLUDED.id_nation, 
                       nation = EXCLUDED.nation, 
                       population = EXCLUDED.population,
                       slug_nation = EXCLUDED.slug_nation";

            using (var cmd = new NpgsqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("IdNation", idNation);
                cmd.Parameters.AddWithValue("Nation", nation);
                cmd.Parameters.AddWithValue("IdYear", idYear);
                cmd.Parameters.AddWithValue("Year", year);
                cmd.Parameters.AddWithValue("Population", population);
                cmd.Parameters.AddWithValue("SlugNation", slugNation);
                cmd.ExecuteNonQuery();
            }
        }

        static void ReadData(NpgsqlConnection conn)
        {
            string sql = "SELECT * FROM population_data";
            using (var cmd = new NpgsqlCommand(sql, conn))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.WriteLine($"ID: {reader.GetInt32(0)}, ID Nation: {reader.GetString(1)}, Nation: {reader.GetString(2)}, ID Year: {reader.GetInt32(3)}, " +
                            $"Year: {reader.GetString(4)}, Population: {reader.GetInt64(5)}, Slug Nation: {reader.GetString(6)}");
                    }
                }
            }
        }
    }
}

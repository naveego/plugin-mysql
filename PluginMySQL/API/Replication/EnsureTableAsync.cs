using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginMySQL.API.Factory;
using PluginMySQL.DataContracts;

namespace PluginMySQL.API.Replication
{
    public static partial class Replication
    {
        private static readonly string EnsureQuery = @"SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_schema = @schema 
AND table_name = @table";

        public static async Task EnsureTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(EnsureQuery, conn);
            cmd.AddParameter("@schema", table.SchemaName);
            cmd.AddParameter("@table", table.TableName);

            // check if table exists
            var reader = await cmd.ExecuteReaderAsync();

            if (!reader.HasRows())
            {
                // create table
                var querySb = new StringBuilder(@"CREATE TABLE IF NOT EXISTS @schema.@table (");
                var primaryKeySb = new StringBuilder("PRIMARY KEY (");
                var hasPrimaryKey = false;
                foreach (var column in table.Columns)
                {
                    querySb.Append(
                        $"{Utility.Utility.GetSafeName(column.ColumnName)} {column.DataType}{(column.PrimaryKey ? " NOT NULL UNIQUE" : "")},");
                    if (column.PrimaryKey)
                    {
                        primaryKeySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)},");
                        hasPrimaryKey = true;
                    }
                }

                if (hasPrimaryKey)
                {
                    primaryKeySb.Length--;
                    primaryKeySb.Append(")");
                    querySb.Append($"{primaryKeySb});");
                }
                else
                {
                    querySb.Length--;
                    querySb.Append(");");
                }

                var query = querySb.ToString();

                cmd = connFactory.GetCommand(query, conn);

                await cmd.ExecuteNonQueryAsync();
            }
            
            await conn.CloseAsync();
        }
    }
}
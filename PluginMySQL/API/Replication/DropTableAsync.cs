using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginMySQL.API.Factory;
using PluginMySQL.DataContracts;

namespace PluginMySQL.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DropTableQuery = @"DROP TABLE IF EXISTS {0}.{1}";

        public static async Task DropTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            var cmd = connFactory.GetCommand(
                string.Format(DropTableQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '`'),
                    Utility.Utility.GetSafeName(table.TableName, '`')
                ),
                conn);
            await cmd.ExecuteNonQueryAsync();

            await conn.CloseAsync();
        }
    }
}
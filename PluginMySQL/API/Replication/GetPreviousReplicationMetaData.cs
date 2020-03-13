using System;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginMySQL.API.Factory;
using PluginMySQL.DataContracts;
using PluginMySQL.Helper;
using Constants = PluginMySQL.API.Utility.Constants;

namespace PluginMySQL.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetMetaDatQuery = $@"SELECT * FROM @schema.@table 
WHERE {Constants.ReplicationMetaDataJobId} = @jobId";

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaData(IConnectionFactory connFactory,
            ReplicationTable metaDataTable)
        {
            try
            {
                ReplicationMetaData replicationMetaData = null;

                // ensure replication metadata table
                await EnsureTableAsync(connFactory, metaDataTable);

                // check if metadata exists
                var conn = connFactory.GetConnection();
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(GetMetaDatQuery, conn);
                var reader = await cmd.ExecuteReaderAsync();

                if (reader.HasRows())
                {
                    // metadata exists
                    replicationMetaData = new ReplicationMetaData
                    {
                        Request = JsonConvert.DeserializeObject<PrepareWriteRequest>(
                            reader.GetValueById(Constants.ReplicationMetaDataRequest).ToString()),
                        ReplicatedShapeName = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeName)
                            .ToString(),
                        ReplicatedShapeId = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeId)
                            .ToString(),
                        Timestamp = DateTime.Parse(reader.GetValueById(Constants.ReplicationMetaDataTimestamp)
                            .ToString())
                    };
                }

                await conn.CloseAsync();

                return replicationMetaData;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }
    }
}
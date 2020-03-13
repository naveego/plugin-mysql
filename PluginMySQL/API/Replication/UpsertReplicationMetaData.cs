using System;
using System.Threading.Tasks;
using PluginMySQL.API.Factory;
using PluginMySQL.API.Utility;
using PluginMySQL.DataContracts;
using PluginMySQL.Helper;

namespace PluginMySQL.API.Replication
{
    public static partial class Replication
    {
        private static readonly string InsertMetaDataQuery = $@"INSERT INTO @schema.@table 
(
{Constants.ReplicationMetaDataJobId}
, {Constants.ReplicationMetaDataRequest}
, {Constants.ReplicationMetaDataReplicatedShapeId}
, {Constants.ReplicationMetaDataReplicatedShapeName}
, {Constants.ReplicationMetaDataTimestamp})
VALUES (
@jobId
, @request
, @shapeId
, @shapeName
, @timestamp
)";
        
        private static readonly string UpdateMetaDataQuery = $@"UPDATE @schema.@table
SET 
{Constants.ReplicationMetaDataRequest} = @request
, {Constants.ReplicationMetaDataReplicatedShapeId} = @shapeId
, {Constants.ReplicationMetaDataReplicatedShapeName} = @shapeName
, {Constants.ReplicationMetaDataTimestamp} = @timestamp
WHERE {Constants.ReplicationMetaDataJobId} = @jobId";
        
        public static async Task UpsertReplicationMetaData(IConnectionFactory connFactory, ReplicationTable table, ReplicationMetaData metaData)
        {
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();
            
            try
            {
                // try to insert
                var cmd = connFactory.GetCommand(InsertMetaDataQuery, conn);
                cmd.AddParameter("@schema", table.SchemaName);
                cmd.AddParameter("@table", table.TableName);
                cmd.AddParameter("@jobId", metaData.Request.DataVersions.JobId);
                cmd.AddParameter("@shapeId", metaData.ReplicatedShapeId);
                cmd.AddParameter("@shapeName", metaData.ReplicatedShapeName);
                cmd.AddParameter("@timestamp", metaData.Timestamp);
                
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                try
                {
                    // update if it failed
                    var cmd = connFactory.GetCommand(UpdateMetaDataQuery, conn);
                    cmd.AddParameter("@schema", table.SchemaName);
                    cmd.AddParameter("@table", table.TableName);
                    cmd.AddParameter("@jobId", metaData.Request.DataVersions.JobId);
                    cmd.AddParameter("@shapeId", metaData.ReplicatedShapeId);
                    cmd.AddParameter("@shapeName", metaData.ReplicatedShapeName);
                    cmd.AddParameter("@timestamp", metaData.Timestamp);
                
                    await cmd.ExecuteNonQueryAsync();
                }
                catch (Exception exception)
                {
                    Logger.Error($"Error Insert: {e.Message}");
                    Logger.Error($"Error Update: {exception.Message}");
                    throw;
                }
            }

            await conn.CloseAsync();
        }
    }
}
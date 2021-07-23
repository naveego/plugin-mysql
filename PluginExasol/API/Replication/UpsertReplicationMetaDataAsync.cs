using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using PluginExasol.API.Factory;
using PluginExasol.API.Utility;
using PluginExasol.DataContracts;
using PluginExasol.Helper;

namespace PluginExasol.API.Replication
{
    public static partial class Replication
    {
        private static readonly string InsertMetaDataQuery = $@"INSERT INTO {{0}}.{{1}} 
(
{Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)})
VALUES (
'{{2}}'
, '{{3}}'
, '{{4}}'
, '{{5}}'
, '{{6}}'
)";

        private static readonly string UpdateMetaDataQuery = $@"UPDATE {{0}}.{{1}}
SET 
{Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)} = '{{2}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)} = '{{3}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)} = '{{4}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)} = '{{5}}'
WHERE {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)} = '{{6}}'";

        public static async Task UpsertReplicationMetaDataAsync(IConnectionFactory connFactory, ReplicationTable table,
            ReplicationMetaData metaData)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                // try to insert
                var cmd = connFactory.GetCommand(
                    string.Format(InsertMetaDataQuery,
                        Utility.Utility.GetSafeName(table.SchemaName, '"'),
                        Utility.Utility.GetSafeName(table.TableName, '"'),
                        metaData.Request.DataVersions.JobId,
                        JsonConvert.SerializeObject(metaData.Request),
                        metaData.ReplicatedShapeId,
                        metaData.ReplicatedShapeName,
                        metaData.Timestamp
                    ),
                    conn);

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                try
                {
                    // update if it failed
                    var cmd = connFactory.GetCommand(
                        string.Format(UpdateMetaDataQuery,
                            Utility.Utility.GetSafeName(table.SchemaName, '"'),
                            Utility.Utility.GetSafeName(table.TableName, '"'),
                            JsonConvert.SerializeObject(metaData.Request),
                            metaData.ReplicatedShapeId,
                            metaData.ReplicatedShapeName,
                            metaData.Timestamp,
                            metaData.Request.DataVersions.JobId
                        ),
                        conn);

                    await cmd.ExecuteNonQueryAsync();
                }
                catch (Exception exception)
                {
                    Logger.Error(e, $"Error Insert: {e.Message}");
                    Logger.Error(exception, $"Error Update: {exception.Message}");
                    throw;
                }
                finally
                {
                    await conn.CloseAsync();
                }
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}
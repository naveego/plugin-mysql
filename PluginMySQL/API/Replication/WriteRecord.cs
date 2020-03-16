using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginMySQL.API.Factory;
using PluginMySQL.API.Utility;
using PluginMySQL.DataContracts;
using PluginMySQL.Helper;

namespace PluginMySQL.API.Replication
{
    public static partial class Replication
    {
        private static readonly SemaphoreSlim ReplicationSemaphoreSlim = new SemaphoreSlim(10, 10);
        
        /// <summary>
        /// Adds and removes records to replication db
        /// Adds and updates available shapes
        /// </summary>
        /// <param name="connFactory"></param>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <param name="config"></param>
        /// <param name="responseStream"></param>
        /// <returns>Error message string</returns>
        public static async Task<string> WriteRecord(IConnectionFactory connFactory, Schema schema, Record record,
            ConfigureReplicationFormData config, IServerStreamWriter<RecordAck> responseStream)
        {
            // // debug
            // Logger.Debug($"Starting timer for {record.RecordId}");
            // var timer = Stopwatch.StartNew();
            //
            // try
            // {
            //     // debug
            //     Logger.Debug(JsonConvert.SerializeObject(record, Formatting.Indented));
            //     
            //     // semaphore
            //     await ReplicationSemaphoreSlim.WaitAsync();
            //
            //     // setup
            //     var safeShapeName = schema.Name;
            //     var safeGoldenBucketName =
            //         string.Concat(config.GoldenBucketName.Where(c => !char.IsWhiteSpace(c)));
            //     var safeVersionBucketName =
            //         string.Concat(config.VersionBucketName.Where(c => !char.IsWhiteSpace(c)));
            //
            //     var goldenBucket = await connFactory.GetBucketAsync(safeGoldenBucketName);
            //     var versionBucket = await connFactory.GetBucketAsync(safeVersionBucketName);
            //
            //     // transform data
            //     var recordVersionIds = record.Versions.Select(v => v.RecordId).ToList();
            //     var recordData = GetNamedRecordData(schema, record.DataJson);
            //     recordData[Constants.ReplicationVersionIds] = recordVersionIds;
            //
            //     // get previous golden record
            //     List<string> previousRecordVersionIds;
            //     if (await goldenBucket.ExistsAsync(record.RecordId))
            //     {
            //         var result = await goldenBucket.GetAsync<Dictionary<string, object>>(record.RecordId);
            //
            //         if (result.Value.ContainsKey(Constants.ReplicationVersionIds))
            //         {
            //             previousRecordVersionIds =
            //                 JsonConvert.DeserializeObject<List<string>>(
            //                     JsonConvert.SerializeObject(result.Value[Constants.ReplicationVersionIds]));
            //         }
            //         else
            //         {
            //             previousRecordVersionIds = recordVersionIds;
            //         }
            //     }
            //     else
            //     {
            //         previousRecordVersionIds = recordVersionIds;
            //     }
            //
            //     // write data
            //     if (recordData.Count == 0)
            //     {
            //         // delete everything for this record
            //         Logger.Debug($"shapeId: {safeShapeName} | recordId: {record.RecordId} - DELETE");
            //         var result = await goldenBucket.RemoveAsync(record.RecordId);
            //         result.EnsureSuccess();
            //
            //         foreach (var versionId in previousRecordVersionIds)
            //         {
            //             Logger.Debug(
            //                 $"shapeId: {safeShapeName} | recordId: {record.RecordId} | versionId: {versionId} - DELETE");
            //             result = await versionBucket.RemoveAsync(versionId);
            //             result.EnsureSuccess();
            //         }
            //     }
            //     else
            //     {
            //         // update record and remove/add versions
            //         Logger.Debug($"shapeId: {safeShapeName} | recordId: {record.RecordId} - UPSERT");
            //         var result = await goldenBucket.UpsertAsync(record.RecordId, recordData);
            //         result.EnsureSuccess();
            //
            //         // delete missing versions
            //         var missingVersions = previousRecordVersionIds.Except(recordVersionIds);
            //         foreach (var versionId in missingVersions)
            //         {
            //             Logger.Debug(
            //                 $"shapeId: {safeShapeName} | recordId: {record.RecordId} | versionId: {versionId} - DELETE");
            //             var versionDeleteResult = await versionBucket.RemoveAsync(versionId);
            //             versionDeleteResult.EnsureSuccess();
            //         }
            //
            //         // upsert other versions
            //         foreach (var version in record.Versions)
            //         {
            //             Logger.Debug(
            //                 $"shapeId: {safeShapeName} | recordId: {record.RecordId} | versionId: {version.RecordId} - UPSERT");
            //             var versionData = GetNamedRecordData(schema, version.DataJson);
            //             var versionUpsertResult = await versionBucket.UpsertAsync(version.RecordId, versionData);
            //             versionUpsertResult.EnsureSuccess();
            //         }
            //     }
            //
            //     var ack = new RecordAck
            //     {
            //         CorrelationId = record.CorrelationId,
            //         Error = ""
            //     };
            //     await responseStream.WriteAsync(ack);
            //
            //     timer.Stop();
            //     Logger.Debug($"Acknowledged Record {record.RecordId} time: {timer.ElapsedMilliseconds}");
            //
            //     return "";
            // }
            // catch (Exception e)
            // {
            //     Logger.Error($"Error replicating records {e.Message}");
            //     // send ack
            //     var ack = new RecordAck
            //     {
            //         CorrelationId = record.CorrelationId,
            //         Error = e.Message
            //     };
            //     await responseStream.WriteAsync(ack);
            //
            //     timer.Stop();
            //     Logger.Debug($"Failed Record {record.RecordId} time: {timer.ElapsedMilliseconds}");
            //
            //     return e.Message;
            // }
            // finally
            // {
            //     ReplicationSemaphoreSlim.Release();
            // }

            // just to compile
            return "";
        }

        /// <summary>
        /// Converts data object with ids to friendly names
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="dataJson"></param>
        /// <returns>Data object with friendly name keys</returns>
        private static Dictionary<string, object> GetNamedRecordData(Schema schema, string dataJson)
        {
            var namedData = new Dictionary<string, object>();
            var recordData = JsonConvert.DeserializeObject<Dictionary<string, object>>(dataJson);

            foreach (var property in schema.Properties)
            {
                var key = property.Id;
                if (!recordData.ContainsKey(key))
                {
                    continue;
                }

                namedData.Add(property.Name, recordData[key]);
            }

            return namedData;
        }
    }
}
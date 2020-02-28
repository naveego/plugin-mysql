using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using Naveego.Sdk.Plugins;
using PluginMySQL.API.Factory;
using PluginMySQL.DataContracts;
using PluginMySQL.Helper;
using Constants = PluginMySQL.API.Utility.Constants;

namespace PluginMySQL.API.Replication
{
    public static partial class Replication
    {
        public static async Task<ReplicationMetaData> GetPreviousReplicationMetadata(IConnectionFactory connFactory,
            PrepareWriteRequest request)
        {
            try
            {
                // ensure replication metadata table


                // check if metadata exists
                if (!await bucket.ExistsAsync(jobId))
                {
                    // no metadata
                    return null;
                }

                // metadata exists


                await conn.CloseAsync();

                return result.Value;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                throw;
            }
        }

        private static ReplicationTable ReplicationMetaDataTable = new ReplicationTable
        {
            TableName = Constants.ReplicationMetaDataTableName,
            Columns = new List<ReplicationColumn>
            {
                new ReplicationColumn
                {
                    ColumnName = "`JobID`",
                    DataType = "",
                    PrimaryKey = false
                },
                new ReplicationColumn
                {
                    ColumnName = "",
                    DataType = "",
                    PrimaryKey = false
                },
                new ReplicationColumn
                {
                    ColumnName = "",
                    DataType = "",
                    PrimaryKey = false
                },
                new ReplicationColumn
                {
                    ColumnName = "",
                    DataType = "",
                    PrimaryKey = false
                },
                new ReplicationColumn
                {
                    ColumnName = "",
                    DataType = "",
                    PrimaryKey = false
                },
            }
        };
    }
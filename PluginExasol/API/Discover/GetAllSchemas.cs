using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginExasol.API.Factory;
using PluginExasol.Helper;

namespace PluginExasol.API.Discover
{
    public static partial class Discover
    {
        private const string TableName = "COLUMN_TABLE";
        private const string TableSchema = "COLUMN_SCHEMA";
        private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "COLUMN_NAME";
        private const string DataType = "COLUMN_TYPE";
        private const string ColumnKey = "CONSTRAINT_TYPE";
        private const string IsNullable = "COLUMN_IS_NULLABLE";
        private const string CharacterMaxLength = "COLUMN_MAXSIZE";

        private const string GetAllTablesAndColumnsQuery = @"SELECT 
            c.COLUMN_TABLE,
            c.COLUMN_SCHEMA,
            c.COLUMN_NAME,
            c.COLUMN_TYPE,
            c.COLUMN_IS_DISTRIBUTION_KEY,
            c.COLUMN_IS_NULLABLE,
            c.COLUMN_MAXSIZE,
            s.CONSTRAINT_TYPE
            FROM SYS.EXA_ALL_COLUMNS as c
            LEFT JOIN SYS.EXA_ALL_CONSTRAINT_COLUMNS as s ON 
            (c.COLUMN_TABLE = s.CONSTRAINT_TABLE AND 
            c.COLUMN_NAME= s.COLUMN_NAME AND
            c.COLUMN_SCHEMA = s.CONSTRAINT_SCHEMA)
            WHERE s.CONSTRAINT_TYPE IS NULL
            OR s.CONSTRAINT_TYPE =  'PRIMARY KEY'
            ORDER BY c.COLUMN_SCHEMA, c.COLUMN_TABLE";
        

        public static async IAsyncEnumerable<Schema> GetAllSchemas(IConnectionFactory connFactory, int sampleSize = 5)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();
                
                var cmd = connFactory.GetCommand(GetAllTablesAndColumnsQuery, conn);

                var reader = await cmd.ExecuteReaderAsync();

                Schema schema = null;
                var currentSchemaId = "";
                while (await reader.ReadAsync())
                {
                    var schemaId =
                        $"{Utility.Utility.GetSafeName(reader.GetValueById(TableSchema).ToString())}.{Utility.Utility.GetSafeName(reader.GetValueById(TableName).ToString())}";
                    if (schemaId != currentSchemaId)
                    {
                        // return previous schema
                        if (schema != null)
                        {
                            // get sample and count
                            if (sampleSize > 0)
                            {
                                yield return await AddSampleAndCount(connFactory, schema, sampleSize);
                            }
                            else
                            {
                                yield return schema;
                            }
                        }

                        // start new schema
                        currentSchemaId = schemaId;
                        var parts = DecomposeSafeName(currentSchemaId).TrimEscape();
                        schema = new Schema
                        {
                            Id = currentSchemaId,
                            Name = $"{parts.Schema}.{parts.Table}",
                            Properties = { },
                            DataFlowDirection = Schema.Types.DataFlowDirection.Read
                        };
                    }

                    // add column to schema
                    var property = new Property
                    {
                        Id = $"{reader.GetValueById(ColumnName)}",
                        Name = reader.GetValueById(ColumnName).ToString(),
                        IsKey = reader.GetValueById(ColumnKey)?.ToString() == "PRIMARY KEY",
                        IsNullable = Boolean.Parse(reader.GetValueById(IsNullable).ToString()),
                        Type = GetType(reader.GetValueById(DataType).ToString()),
                        TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString(),
                            reader.GetValueById(CharacterMaxLength))
                    };
                    schema?.Properties.Add(property);
                }

                if (schema != null)
                {
                    // get sample and count
                    if (sampleSize > 0)
                    {
                        yield return await AddSampleAndCount(connFactory, schema, sampleSize);
                    }
                    else
                    {
                        yield return schema;
                    }
                }
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        private static async Task<Schema> AddSampleAndCount(IConnectionFactory connFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(connFactory, schema).Take(10);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(connFactory, schema);

            
            return schema;
        }

        public static PropertyType GetType(string dataType)
        {
            switch (dataType.ToUpper())
            {
                case string a when a.Contains("CHAR"): 
                    return PropertyType.String;
                case string b when b.Contains("DECIMAL"):
                    return PropertyType.Decimal;
                case string c when c.Contains("DATE"):
                    return PropertyType.Date;
                case string d when d.Contains("TIMESTAMP"):
                    return PropertyType.Datetime;
                case string d when d.Contains("DOUBLE"):
                    return PropertyType.Float;
                default:
                    return PropertyType.String;
                
                // case "datetime":
                // case "timestamp":
                //     return PropertyType.Datetime;
                // case "date":
                //     return PropertyType.Date;
                // case "time":
                //     return PropertyType.Time;
                // case "tinyint":
                // case "smallint":
                // case "mediumint":
                // case "int":
                // case "bigint":
                //     return PropertyType.Integer;
                // case "decimal":
                //     return PropertyType.Decimal;
                // case "float":
                // case "double":
                //     return PropertyType.Float;
                // case "boolean":
                //     return PropertyType.Bool;
                // case "blob":
                // case "mediumblob":
                // case "longblob":
                //     return PropertyType.Blob;
                // case "char":
                // case "varchar":
                // case "tinytext":
                //     return PropertyType.String;
                // case "text":
                // case "mediumtext":
                // case "longtext":
                //     return PropertyType.Text;
                // default:
                //     return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}" : dataType;
        }
    }
}
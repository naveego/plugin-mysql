using System.Collections.Generic;
using System.Threading.Tasks;
using PluginExasol.API.Factory;
using PluginExasol.DataContracts;

namespace PluginExasol.API.Write
{
    public static partial class Write
    {
        private const string SchemaName = "ROUTINE_SCHEMA";
        private const string RoutineName = "ROUTINE_NAME";
        private const string SpecificName = "SPECIFIC_NAME";

        private static string GetAllStoredProceduresQuery = @"
SELECT FUNCTION_SCHEMA, FUNCTION_NAME
FROM sys.EXA_ALL_FUNCTIONS";

        public static async Task<List<WriteStoredProcedure>> GetAllStoredProceduresAsync(IConnectionFactory connFactory)
        {
            var storedProcedures = new List<WriteStoredProcedure>();
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(GetAllStoredProceduresQuery, conn);
                var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var storedProcedure = new WriteStoredProcedure
                    {
                        SchemaName = reader.GetValueById(SchemaName).ToString(),
                        RoutineName = reader.GetValueById(RoutineName).ToString(),
                        SpecificName = reader.GetValueById(SpecificName).ToString()
                    };

                    storedProcedures.Add(storedProcedure);
                }

                return storedProcedures;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}
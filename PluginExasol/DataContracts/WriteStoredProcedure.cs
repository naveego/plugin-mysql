using PluginExasol.API.Utility;

namespace PluginExasol.DataContracts
{
    public class WriteStoredProcedure
    {
        public string SchemaName { get; set; }
        public string RoutineName { get; set; }
        public string SpecificName { get; set; }

        public string GetId()
        {
            return $"{Utility.GetSafeName(SchemaName)}.{Utility.GetSafeName(RoutineName)}";
        }
    }
}
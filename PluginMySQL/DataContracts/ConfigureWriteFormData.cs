namespace PluginMySQL.DataContracts
{
    public class ConfigureWriteFormData
    {
        public string StoredProcedure { get; set; }
        public string GoldenRecordIdParam { get; set; }
    }
}
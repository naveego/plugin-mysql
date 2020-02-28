using System.Collections.Generic;

namespace PluginMySQL.DataContracts
{
    public class ReplicationTable
    {
        public string Schema { get; set; }
        public string TableName { get; set; }
        public List<ReplicationColumn> Columns { get; set; }
    }
}
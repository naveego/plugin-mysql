using System;
using System.Data;
using System.Data.Odbc;
using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;
using PluginExasol.Helper;
using Exasol.EXADataProvider;

namespace PluginExasol.API.Factory
{
    public class Connection : IConnection
    {
        private readonly EXAConnection _conn;

        
        public Connection(Settings settings)
        {
            _conn = new EXAConnection();
            _conn.ConnectionString = settings.GetConnectionString();
        }

        public Connection(Settings settings, string database)
        {
            _conn = new EXAConnection();
            
            _conn.ConnectionString = settings.GetConnectionString();
        }

        public async Task OpenAsync()
        {
            await _conn.OpenAsync();
        }

        public async Task CloseAsync()
        {
            await _conn.CloseAsync();
        }

        public async Task<bool> PingAsync()
        {
            EXACommand cmd = new EXACommand();
            cmd.CommandText = "SELECT 1";
            cmd.Connection = _conn;

            var result = await cmd.ExecuteReaderAsync();

            return result.HasRows;
        }

        public EXAConnection GetConnection()
        {
            return _conn;
        }
    }
}
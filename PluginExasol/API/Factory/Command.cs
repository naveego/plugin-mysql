using System.Threading.Tasks;
using Exasol.EXADataProvider;
using MySqlConnector;

namespace PluginExasol.API.Factory
{
    public class Command : ICommand
    {
        private readonly EXACommand _cmd;

        public Command()
        {
            _cmd = new EXACommand();
        }

        //remove this?
        // public Command(string commandText)
        // {
        //     _cmd = new EXACommand();
        //     _cmd.CommandText = commandText;
        // }

        public Command(string commandText, IConnection conn)
        {
            _cmd = new EXACommand();
            _cmd.CommandText = commandText;
            _cmd.Connection = conn.GetConnection();
        }

        public void SetConnection(IConnection conn)
        {
            _cmd.Connection = conn.GetConnection();
        }

        public void SetCommandText(string commandText)
        {
            _cmd.CommandText = commandText;
        }

        public void AddParameter(string name, object value)
        {
            _cmd.Parameters.Add(value);
            // _cmd.Parameters.AddWithValue(name, value);
        }

        public async Task<IReader> ExecuteReaderAsync()
        {
            return new Reader(await _cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            return await _cmd.ExecuteNonQueryAsync();
        }
    }
}
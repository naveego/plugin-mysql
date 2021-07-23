using System.Data;
using Exasol.EXADataProvider;
using PluginExasol.Helper;

namespace PluginExasol.API.Factory
{
    public class ConnectionFactory : IConnectionFactory
    {
        private Settings _settings;

        public void Initialize(Settings settings)
        {
            _settings = settings;
        }

        public IConnection GetConnection()
        {
            return new Connection(_settings);
        }

        public IConnection GetConnection(string database)
        {
            return new Connection(_settings, database);
        }

        public ICommand GetCommand(string commandText, IConnection connection)
        {
            return new Command(commandText, connection);
        }
    }
}
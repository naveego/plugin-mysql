using System.Data;
using System.Threading.Tasks;
using Exasol.EXADataProvider;

namespace PluginExasol.API.Factory
{
    public interface IConnection
    {
        Task OpenAsync();
        Task CloseAsync();
        Task<bool> PingAsync();
        EXAConnection GetConnection();
    }
}
using System.Net.Sockets;

namespace DistributedWAL.Networking;

internal class ConnectionListener : IDisposable
{
    private readonly IConsensus _consensus;
    private readonly int _port;
    TcpListener _listener;

    public ConnectionListener(IConsensus consensus, int port)
    {
        _consensus = consensus;
        _port = port;
        _listener = TcpListener.Create(_port);
        _listener.Start();
    }

    public void Dispose()
    {
        _listener.Stop();
    }

    internal Connection AcceptConnection()
    {
        return new Connection(_listener.AcceptTcpClient());
    }
}

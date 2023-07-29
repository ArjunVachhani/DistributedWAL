using System.Net;
using System.Net.Sockets;

namespace DistributedWAL;

internal class DicoveryConnectionListener
{
    private readonly ConnectionConfiguration _connectionConfiguration;
    private bool _stopping;
    private int _openConnection = 0;
    public DicoveryConnectionListener(ConnectionConfiguration connectionConfiguration)
    {
        _connectionConfiguration = connectionConfiguration;
    }

    public void Stop()
    {
        _stopping = true;
    }

    private async void Start()
    {
        TcpListener tcpListener = new TcpListener(IPAddress.Any, _connectionConfiguration.DiscoveryPort);
        tcpListener.Start(10);
        while (!_stopping)
        {
            if (_openConnection <= 10)
            {
                var tcpClient = await tcpListener.AcceptTcpClientAsync();
                Thread t = new Thread(Serve);
                t.Priority = ThreadPriority.Highest;
                t.Start();
                Interlocked.Increment(ref _openConnection);
            }
            else
                Thread.Sleep(100);
        }
        tcpListener.Stop();
    }

    private void Serve(object? tcpClient)
    {
        if (tcpClient is TcpClient)
            Serve((TcpClient)tcpClient);
    }

    private void Serve(TcpClient tcpClient)
    {
        try
        {
            var stream = tcpClient.GetStream();

        }
        catch (Exception)
        {
            Interlocked.Decrement(ref _openConnection);
            tcpClient.Dispose();
        }
    }
}

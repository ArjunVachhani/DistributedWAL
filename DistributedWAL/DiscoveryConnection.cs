using Microsoft.Extensions.Logging;
using System.Net.Sockets;

namespace DistributedWAL;

internal class DiscoveryConnection
{
    private readonly List<ConnectionConfiguration> _dicoveredConfigurations = new List<ConnectionConfiguration>();
    private readonly ConnectionConfiguration _connectionConfiguration;
    private readonly ILogger<DiscoveryConnection> _logger;
    private TcpClient? _tcpClient;
    private bool _connected;
    private bool _stopping;

    public IEnumerable<ConnectionConfiguration> DicoveredConnection => Connected ? _dicoveredConfigurations : Enumerable.Empty<ConnectionConfiguration>();
    public ConnectionConfiguration ConnectionConfiguration => _connectionConfiguration;
    public bool Connected => !_stopping && _connected;

    public DiscoveryConnection(ConnectionConfiguration connectionConfiguration, ILogger<DiscoveryConnection> logger)
    {
        _connectionConfiguration = connectionConfiguration;
        _logger = logger;
        Thread t = new Thread(Start);
        t.Priority = ThreadPriority.Highest;
        t.Start();
    }

    public void Stop()
    {
        _stopping = true;
    }

    private void Start()
    {
        while (!_stopping)
        {
            Thread.Sleep(100);
            if (_tcpClient == null)
                _tcpClient = TryConnect();

            if (!Ping())
                continue;

            if (!GetPeers())
                continue;

        }
        _tcpClient?.Dispose();
    }

    private TcpClient? TryConnect()
    {
        while (!_stopping)
        {
            try
            {
                var connection = new TcpClient(_connectionConfiguration.Host, _connectionConfiguration.DiscoveryPort);
                //TODO check cluster name, key
                _connected = true;
                return connection;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to connect dicovery endpoint {Host} {DiscoveryPort}", _connectionConfiguration.Host, _connectionConfiguration.DiscoveryPort);
                Thread.Sleep(1000);
            }
        }
        return null;
    }

    private bool Ping()
    {
        try
        {
            var stream = _tcpClient!.GetStream();

            stream.Write(DicoveryConnectionMessageCodes.Ping);

            var random = Random.Shared.Next();
            stream.Write(random);

            _tcpClient.ReceiveTimeout = 5000;
            var resultCode = stream.ReadInt32();
            if (resultCode != DicoveryConnectionMessageCodes.Ping)
            {
                _logger.LogError("Expected ping message back. Expected {ExpectedCode} Got {ResultCode}", DicoveryConnectionMessageCodes.Ping, resultCode);
                CloseDisconnect();
                return false;
            }

            var pingBack = stream.ReadInt32();
            if (pingBack != random)
            {
                _logger.LogError("Got invalid ping back. Disconnecting DicoveryConnection");
                CloseDisconnect();
                return false;
            }

            _tcpClient.ReceiveTimeout = 0;
            return true;
        }
        catch (Exception)
        {
            CloseDisconnect();
            return false;
        }
    }

    private bool GetPeers()
    {
        try
        {
            var stream = _tcpClient!.GetStream();

            stream.Write(DicoveryConnectionMessageCodes.Peers);

            var random = Random.Shared.Next();
            stream.Write(random);

            _tcpClient.ReceiveTimeout = 5000;
            var resultCode = stream.ReadInt32();
            if (resultCode != DicoveryConnectionMessageCodes.Peers)
            {
                _logger.LogError("Expected peers message back. Expected {ExpectedCode} Got {ResultCode}", DicoveryConnectionMessageCodes.Peers, resultCode);
                CloseDisconnect();
                return false;
            }

            var responseRandom = stream.ReadInt32();
            if (responseRandom != random)
            {
                _logger.LogError("Got invalid peers back. Disconnecting DicoveryConnection");
                CloseDisconnect();
                return false;
            }

            var peersCount = stream.ReadInt32();
            _dicoveredConfigurations.Clear();
            for (int i = 0; i < peersCount; i++)
            {
                var hostName = stream.ReadUtf8LengthPrefixed();
                var replPort = stream.ReadInt32();
                var discoveryPort = stream.ReadInt32();
                _dicoveredConfigurations.Add(new ConnectionConfiguration(hostName, replPort, discoveryPort));
            }
            _tcpClient.ReceiveTimeout = 0;
            return true;
        }
        catch (Exception)
        {
            CloseDisconnect();
            return false;
        }
    }

    private void CloseDisconnect()
    {
        _tcpClient?.Close();
        _tcpClient = null;
        _connected = false;
    }
}

internal class DicoveryConnectionMessageCodes
{
    public const int ConnectionCheck = 10001;
    public const int Ping = 10002;
    public const int Peers = 10003;
}

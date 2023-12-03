using System.Net.Sockets;

namespace DistributedWAL.Networking;

internal class Connection : IDisposable
{
    private readonly TcpClient _tcpClient;
    private readonly Stream _stream;
    public Connection(TcpClient tcpClient)
    {
        _tcpClient = tcpClient;
        _stream = tcpClient.GetStream();
    }

    internal void Read(Span<byte> bytes)
    {
        _stream.ReadExactly(bytes);
    }

    internal void Write(ReadOnlySpan<byte> bytes)
    {
        _stream.Write(bytes);
    }

    public void Dispose()
    {
        _stream.Dispose();
        _tcpClient.Close();
    }
}

using System.Buffers;
using System.Net.Sockets;
using DistributedWAL.Storage;

namespace DistributedWAL.Networking;

internal class Sender : IDisposable
{
    private readonly object _lock = new object();
    //TODO it should have logic to reconnect
    private const int DefaultBufferSize = 65536;

    private readonly int _term;
    private readonly IConsensus _consensus;
    private readonly IFileProvider _fileProvider;
    private readonly string _host;
    private readonly int _port;

    private TcpClient? _tcpClient;

    public Sender(int term, IConsensus consensus, string host, int port, IFileProvider fileProvider)
    {
        _term = term;
        _consensus = consensus;
        _fileProvider = fileProvider;
        _host = host;
        _port = port;
    }

    private void Writer()
    {
        using var fileReader = _fileProvider.GetFileForRead(_consensus.CommittedLogIndex);
        while (_consensus.Term == _term && (_consensus.NodeRole == NodeRoles.Leader || _consensus.NodeRole == NodeRoles.Candidate))
        {
            if (_consensus.NodeRole == NodeRoles.Leader)
            {
                //send logs
            }
            else if (_consensus.NodeRole == NodeRoles.Candidate)
            {
                // request vote
            }
        }
    }

    private TcpClient? GetConnection()
    {
        if (_tcpClient == null)
        {
            lock (_lock)
            {
                if (_tcpClient == null)
                {
                    try
                    {
                        _tcpClient = new TcpClient(_host, _port);
                    }
                    catch (Exception)
                    {
                        //TODO log exception
                    }
                }
            }
        }
        return _tcpClient;
    }

    private void Reader()
    {
        var bytes = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);//TODO should be size of max message size or 65kb
        try
        {
            var tcpClient = GetConnection();
            if (tcpClient != null)
            {
                var stream = tcpClient.GetStream();
                stream.ReadExactly(bytes, 0, 4);
                var messageType = BitConverter.ToInt32(bytes, 4);
                if (messageType == NetworkMessageType.RequestVoteResponse)
                {
                    RequestVoteResponse(stream, bytes);
                }
                else if (messageType == NetworkMessageType.AppendEntriesResponse)
                {
                    AppendEntriesResponse();
                }
                else if (messageType == NetworkMessageType.InstallSnapshotResponse)
                {
                    InstallSnapshotResponse();
                }
                else
                {
                    //unexpected
                    //TODO something bad has happend
                }
            }
        }
        catch (Exception)
        {
            ArrayPool<byte>.Shared.Return(bytes);
            //TODO
            throw;
        }
    }

    private void RequestVoteResponse(Stream stream, byte[] buffer)
    {
        stream.ReadExactly(buffer, 0, 4);
    }

    private void AppendEntriesResponse()
    {
    }

    private void InstallSnapshotResponse()
    {
    }

    public void Dispose()
    {
        _tcpClient?.Dispose();
    }
}

using System.Buffers;
using System.Net.Sockets;

namespace DistributedWAL.Networking;

internal class Receiver : IDisposable
{
    private const int DefaultBufferSize = 65536;

    private readonly TcpClient _tcpClient;
    private readonly IConsensus _consensus;
    private string? remoteHost;
    private int? _term;
    public Receiver(TcpClient tcpClient, IConsensus consensus)
    {
        _tcpClient = tcpClient;
        _consensus = consensus;
    }

    public void Dispose()
    {
        _tcpClient.Dispose();
    }

    public void Reader()
    {
        using var stream = _tcpClient.GetStream();
        var bytes = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
        try
        {
            while (_term == null || _term == _consensus.Term)
            {
                var messageType = NetworkHelper.GetNetworkMessageType(stream, bytes);
                if (messageType == NetworkMessageType.RequestVote)
                {
                    RequestVote(stream, bytes);
                }
                else if (messageType == NetworkMessageType.AppendEntries)
                {
                    var resultCode = AppendEntries(stream, bytes);
                    if (resultCode == AppendEntriesResultCode.HigherTerm)
                        break;
                }
                else if (messageType == NetworkMessageType.InstallSnapshot)
                {
                }
                else
                {
                    //unexpected
                    //May be corrupted data. should halt everything
                    //TODO something bad has happend
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
            _tcpClient.Dispose();
        }
    }

    private void RequestVote(Stream stream, Span<byte> bytes)
    {
        (int remoteTerm, remoteHost, LogNumber logNumber) = NetworkHelper.GetRequestVote(stream, bytes);
        var voteGranted = _consensus.RequestVote(remoteTerm, remoteHost!, logNumber);
        if (voteGranted)
            _term = remoteTerm;

        RequestVoteResponse(stream, bytes, voteGranted);
    }

    private void RequestVoteResponse(Stream stream, Span<byte> bytes, bool voteGranted)
    {
        NetworkHelper.SendRequestVoteResponse(stream, bytes, voteGranted, _consensus.Term);
    }

    private int AppendEntries(Stream stream, Span<byte> bytes)
    {
        (var resultCode, var logIndex) = NetworkHelper.GetAppendEntries(stream, bytes, _consensus);
        AppendEntriesResponse(stream, bytes, resultCode, logIndex);
        return resultCode;
    }

    private void AppendEntriesResponse(Stream stream, Span<byte> bytes, int resultCode, long logIndex)
    {
        NetworkHelper.SendAppendEntriesResponse(stream, bytes, _consensus.Term, resultCode, logIndex);
    }
}

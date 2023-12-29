using DistributedWAL.Storage;
using System.Buffers;
using System.Net.Sockets;

namespace DistributedWAL.Networking;

internal class Sender : IDisposable
{
    private readonly object _lock = new object();
    //TODO it should have logic to reconnect
    private const int DefaultBufferSize = 65536;
    //TODO Max Messages in 1 AppendLog RPC
    private readonly int _term;
    private readonly IConsensus _consensus;
    private readonly string _remoteHost;
    private readonly string _localHost;
    private readonly int _port;
    private int MaxLogsInPipeline = 1;//TODO config for max logs in pipeline
    private TcpClient? _tcpClient;
    private LogNumber _lastSentLogNumber = new LogNumber(-1, -1);//TODO initialize properly
    private long _remoteSavedLogIndex;//TODO initialize properly
    private bool _sendLogs = true;
    private bool firstAppendEntries = true;
    private readonly AutoResetEvent _autoResetEvent = new AutoResetEvent(false);

    public Sender(int term, IConsensus consensus, string localHost, string remoteHost, int port)
    {
        _term = term;
        _consensus = consensus;
        _remoteHost = remoteHost;
        _localHost = localHost;
        _port = port;
    }

    private void Writer()
    {
        using var fileReader = _consensus.GetFileReader(_lastSentLogNumber.LogIndex + 1);
        var bytes = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);//TODO need to consider size again, response message should be short
        try
        {
            while (_consensus.Term == _term && (_consensus.NodeRole == NodeRoles.Leader || _consensus.NodeRole == NodeRoles.Candidate))
            {
                var tcpClient = GetConnection();
                try
                {
                    if (tcpClient != null)
                    {
                        var stream = tcpClient.GetStream();
                        if (_consensus.NodeRole == NodeRoles.Leader)
                        {
                            AppendEntriesOrInstallSnapshot(stream, bytes, fileReader);
                        }
                        else if (_consensus.NodeRole == NodeRoles.Candidate)
                        {
                            RequestVote(stream, bytes);
                        }
                    }
                }
                catch (Exception)
                {
                    tcpClient?.Dispose();
                    //TODO
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }

    private TcpClient? GetConnection()
    {
        //TODO this should handle closed connection. in case of closed connection return create ne connection
        if (_tcpClient == null || !_tcpClient.Connected)
        {
            lock (_lock)
            {
                if (_tcpClient == null || !_tcpClient.Connected)
                {
                    try
                    {
                        _tcpClient?.Dispose();
                        _tcpClient = new TcpClient(_remoteHost, _port);
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

    private void AckNackReader()
    {
        var bytes = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);//TODO need to consider size again, response message should be short
        try
        {
            while (_term == _consensus.Term)//retry reading, continue till node is leader
            {
                var tcpClient = GetConnection();
                try
                {
                    if (tcpClient != null)
                    {
                        var stream = tcpClient.GetStream();
                        var messageType = NetworkHelper.GetNetworkMessageType(stream, bytes);
                        if (messageType == NetworkMessageType.RequestVoteResponse)
                        {
                            RequestVoteResponse(stream, bytes);
                        }
                        else if (messageType == NetworkMessageType.AppendEntriesResponse)
                        {
                            AppendEntriesResponse(stream, bytes);
                        }
                        else if (messageType == NetworkMessageType.InstallSnapshotResponse)
                        {
                            InstallSnapshotResponse();
                        }
                        else
                        {
                            //unexpected
                            //May be corrupted data. should halt everything
                            //TODO something bad has happend
                        }
                    }
                }
                catch (Exception)
                {
                    tcpClient?.Dispose();
                    //TODO
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }

    private void RequestVote(Stream stream, Span<byte> bytes)
    {
        NetworkHelper.SendRequestVote(stream, bytes, _term, _localHost, _consensus.SavedLogNumber);
    }

    private void AppendEntriesOrInstallSnapshot(Stream stream, Span<byte> bytes, IFileReader fileReader)
    {
        if (_sendLogs)
        {
            AppendEntriesRequest(stream, bytes, fileReader);
        }
        else
        {
            //TODO install snapshot
        }
    }

    private void AppendEntriesRequest(Stream stream, Span<byte> bytes, IFileReader fileReader)
    {
        var logsInPipeline = _consensus.SavedLogNumber.LogIndex - _lastSentLogNumber.LogIndex;
        var logCount = logsInPipeline >= MaxLogsInPipeline ? 0 : MaxLogsInPipeline;//TODO batch logic
        var hostName = firstAppendEntries ? _localHost : null;
        NetworkHelper.SendAppendEntries(stream, bytes, logCount, _term, hostName, ref _lastSentLogNumber, _consensus, fileReader);
        firstAppendEntries = false;
    }

    private void RequestVoteResponse(Stream stream, Span<byte> bytes)
    {
        (var term, bool voteGranted) = NetworkHelper.GetRequestVoteResponse(stream, bytes);
        if (voteGranted)
            _consensus.VoteGranted(term);
        else if (term > _term)
        {
            //TODO update term if term is bigger and transit to follower
        }
    }

    private void AppendEntriesResponse(Stream stream, Span<byte> bytes)
    {
        (var senderTerm, int resultCode, long logIndex) = NetworkHelper.GetAppendEntriesResponse(stream, bytes);
        if (resultCode == AppendEntriesResultCode.Success)
        {
            _remoteSavedLogIndex = logIndex;
            _autoResetEvent.Set();
        }
        else if (resultCode == AppendEntriesResultCode.HigherTerm)
        {
            _consensus.UpdateTerm(senderTerm);
        }
        else if (resultCode == AppendEntriesResultCode.MissingLog)
        {

        }
        else
        {
            //TODO unknown 
        }
    }

    private void InstallSnapshotResponse()
    {
    }

    public void Dispose()
    {
        _tcpClient?.Dispose();
    }
}

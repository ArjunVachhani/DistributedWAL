using DistributedWAL.Networking;
using DistributedWAL.Storage;

namespace DistributedWAL;

// this is central point from where new log will be saved.
// it will contain information about logs and node status, if node is leader or not
// this is be central point. all the operations should go throgh this. write log, append log, read log etc
internal partial class Consensus : IConsensus
{
    private int _term;
    private LogNumber _writtenLogNumber = new LogNumber(-1, -1);//TODO initialize
    private long _committedLogIndex = -1;//TODO initialize
    private int _nodeRole;
    private string? _leaderHost;
    private string _currentHostName = "localhost"; //TODO initialize
    private readonly ILogStore _logStore;
    private readonly IFileWriter _fileWriter;

    public int NodeRole => Interlocked.CompareExchange(ref _nodeRole, 0, 0);
    public int Term => Interlocked.CompareExchange(ref _term, 0, 0);
    public long CommittedLogIndex => Interlocked.CompareExchange(ref _committedLogIndex, 0, 0);
    public LogNumber SavedLogNumber => _fileWriter.SavedLogNumber;
    public string? LeaderHost => _leaderHost;
    public string CurrentHostName => _currentHostName;

    public Consensus(DistributedWalConfig config)
    {
        _logStore = new LogStore(config.MaxFileSize, config.WriteBatchTime, config.ReadBatchTime, config.LogDirectory);
        _fileWriter = _logStore.GetFileWriter();

        _term = 0;
        _nodeRole = NodeRoles.Leader; //TODO Not OK
    }

    public LogNumber WriteLog(ReadOnlySpan<byte> data)
    {
        //TODO lock/threadsafe
        if (NodeRole != NodeRoles.Leader)
            throw new DistributedWalException("Node is not a leader.");

        //TODO set the max limit of log size to 32KB

        //TODO we should have some back pressure mechanism to max uncommitted logs when follower are falling behind
        //TODO single producer, restrict concurrent calls
        _writtenLogNumber = new LogNumber(_term, _writtenLogNumber.LogIndex + 1);
        _fileWriter.WriteLog(data, _writtenLogNumber);

        _committedLogIndex = _writtenLogNumber.LogIndex;//TODO _committedLogIndex is should not be hear
        return _writtenLogNumber;
    }

    public IFileReader GetFileReader(long logIndex)
    {
        return _logStore.GetFileReader(logIndex);
    }

    public void VoteGranted(int term)
    {
        //TODO use lock and
        if (_term == term && _nodeRole == NodeRoles.Candidate)
        {
            //TODO increment counter, if Quorum is achieved tansit to leader
        }
    }

    public bool RequestVote(int term, string host, LogNumber logNumber)
    {
        return false;
    }

    public void UpdateTerm(int term)
    {
        if (_term < term)
        {
            _term = term;
            if (NodeRole == NodeRoles.Leader)
                Interlocked.Exchange(ref _nodeRole, NodeRoles.Follower);
        }
    }

    public void Flush()
    {
        _fileWriter.Flush();
        _fileWriter.Stop();
        //_walFileManager.Flush();
    }

    public void Dispose()
    {
        //_walFileManager.Dispose();
    }

    public (int ResultCode, long LogIndex) AppendLog(string? hostName, int term, LogNumber prevLogNumber, ReadOnlySpan<byte> data)
    {
        //TODO add locks
        if (term < _term)
            return (AppendEntriesResultCode.HigherTerm, default);

        if (_term > term)
            _term = term;

        //TODO if not follower transit to follower
        if (!string.IsNullOrWhiteSpace(hostName))
            _leaderHost = hostName;

        //TODO compare prevLogNumber
        //TODO write log to file
        //TODO reset timer
        return (AppendEntriesResultCode.Success, SavedLogNumber.LogIndex);
    }

    public (int ResultCode, long LogIndex) AppendLog(string? hostName, int term, long committedIndex)
    {
        //TODO locking/sync
        if (term < _term)
            return (AppendEntriesResultCode.HigherTerm, default);

        if (!string.IsNullOrWhiteSpace(hostName))
            _leaderHost = hostName;

        //TODO if not follower transit to follower
        //TODO reset timer
        _committedLogIndex = Math.Min(_writtenLogNumber.LogIndex, committedIndex);
        return (AppendEntriesResultCode.Success, SavedLogNumber.LogIndex);
    }
}

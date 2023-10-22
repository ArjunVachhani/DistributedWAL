using System.IO.MemoryMappedFiles;

namespace DistributedWAL;

// this is central point from where new log will be saved.
// it will contain information about logs and node status, if node is leader or not
// this is be central point. all the operations should go throgh this. write log, append log, read log etc
internal partial class Consensus : IDisposable
{
    private int _term;
    private long _nextLogIndex = 0;
    private long _writtenLogIndex = -1;
    private long _committedLogIndex = -1;
    private int _nodeRole;

    private readonly WalFileManager _walFileManager;
    internal int NodeRole => Interlocked.CompareExchange(ref _nodeRole, 0, 0);
    internal int Term => Interlocked.CompareExchange(ref _term, 0, 0);
    internal long CommittedLogIndex => Interlocked.CompareExchange(ref _committedLogIndex, 0, 0);
    internal WalFileManager WalFileManager => _walFileManager;//TODO not ok, it should not expose underlying store

    internal Consensus(DistributedWalConfig config)
    {
        _walFileManager = new WalFileManager(config.MaxFileSize, config.LogDirectory);
        (var lastLogIndex, var lastLogTerm) = _walFileManager.Initialize();
        _term = lastLogTerm;
        _nextLogIndex = lastLogIndex + 1;
        _writtenLogIndex = lastLogIndex;
        _nodeRole = NodeRoles.Leader; //TODO Not OK
    }

    internal LogWriter Copy(int term, long logIndex, int size)
    {
        if (Term < term)
        {
            if (Interlocked.CompareExchange(ref _nodeRole, NodeRoles.Follower, NodeRoles.Leader) == NodeRoles.Leader)
            {
                Interlocked.Exchange(ref _term, term);
                //was leader
            }
        }
        else
        {
            //reject
        }

        return default;
    }

    internal (MemoryMappedViewAccessor viewAccessor, int term, long logIndex, int position) RequestWriteSegment(int length, bool isFixedSize)
    {
        if (_nodeRole != NodeRoles.Leader)
            throw new DistributedWalException("Node is not a leader.");

        //TODO we should have some back pressure mechanism to max uncommited logs when follower are falling behind
        (var viewAccessor, var position) = _walFileManager.RequestWriteSegment(length, isFixedSize);

        var logIndex = Interlocked.Increment(ref _nextLogIndex) - 1;
        return (viewAccessor, _term, logIndex, position);
    }

    internal void FinishedWriting(int startPosition, int endPosition, long logIndex)
    {
        _walFileManager.FinishedWriting(startPosition, endPosition);
        while (Interlocked.CompareExchange(ref _writtenLogIndex, logIndex, logIndex - 1) != logIndex - 1)
        {
            Thread.SpinWait(10);
        }

        //TODO 
        Interlocked.Exchange(ref _committedLogIndex, logIndex);
        _committedLogIndex = _writtenLogIndex;
    }

    internal void CancelWriting()
    {

    }

    internal void RequestVote()
    {
    }

    internal void TimeOut()
    {
        if (_nodeRole != NodeRoles.Leader)
        {
            Interlocked.Increment(ref _term);
            //TODO Voting
        }
    }

    internal void Flush()
    {
        _walFileManager.Flush();
    }

    public void Dispose()
    {
        _walFileManager.Dispose();
    }
}

using System.IO.MemoryMappedFiles;

namespace DistributedWAL;

internal partial class Consensus : IDisposable
{
    private int _term;
    private long _nextLogIndex = 0;
    private long _writtenLogIndex = 0;
    private int _nodeRole;

    private readonly WalFileManager _walFileManager;

    internal int NodeRole => Interlocked.CompareExchange(ref _nodeRole, 0, 0);
    internal int Term => Interlocked.CompareExchange(ref _term, 0, 0);

    internal Consensus(DistributedWalConfig config)
    {
        _walFileManager = new WalFileManager(config.MaxFileSize, config.FilePath);
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
                _nodeRole = NodeRoles.Follower;
                //was leader
            }
        }
        else
        {
            //reject
        }

        return default;
    }

    internal LogWriter AppendFixedSizeLog(int size)
    {
        Interlocked.Increment(ref _nextLogIndex);
        return default;
    }

    internal LogWriter AppendLog(int maxSize)
    {
        Interlocked.Increment(ref _nextLogIndex);
        return default;
    }

    internal (MemoryMappedViewAccessor viewAccessor, int term, long logIndex, int position, long timestamp) RequestWriteSegment(int length, bool isFixedSize)
    {
        if (_nodeRole != NodeRoles.Leader)
            throw new DistributedWalException("Node is not a leader.");
        (var viewAccessor, var position) = _walFileManager.RequestWriteSegment(length, isFixedSize);

        var logIndex = Interlocked.Increment(ref _nextLogIndex) - 1;
        return (viewAccessor, _term, logIndex, position, 0);
    }

    internal void FinishedWriting(int startPosition, int endPosition, long logIndex)
    {
        _walFileManager.FinishedWriting(startPosition, endPosition);
        while (Interlocked.CompareExchange(ref _writtenLogIndex, logIndex, logIndex - 1) != logIndex - 1)
        {
            Thread.SpinWait(10);
        }
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

    public void Dispose()
    {
        _walFileManager.Dispose();
    }
}

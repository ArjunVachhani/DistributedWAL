using DistributedWAL.Storage;

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

    private readonly LogStore _logStore;
    private readonly FileWriter _fileWriter;
    internal int NodeRole => Interlocked.CompareExchange(ref _nodeRole, 0, 0);
    internal int Term => Interlocked.CompareExchange(ref _term, 0, 0);
    internal long CommittedLogIndex => Interlocked.CompareExchange(ref _committedLogIndex, 0, 0);

    internal Consensus(DistributedWalConfig config)
    {
        _logStore = new LogStore(config.MaxFileSize, config.WriteBatchTime, config.ReadBatchTime, config.LogDirectory);
        _fileWriter = _logStore.GetFileWriter();

        _term = 0;
        _nextLogIndex = 0;
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

    internal (BufferSegment bufferSegment, LogNumber logNumber) RequestWriteSegment(int length)
    {
        if (_nodeRole != NodeRoles.Leader)
            throw new DistributedWalException("Node is not a leader.");

        //TODO we should have some back pressure mechanism to max uncommited logs when follower are falling behind
        var bufferSegment = _fileWriter.RequestWriteSegment(length);
        var logIndex = Interlocked.Increment(ref _nextLogIndex) - 1;
        return (bufferSegment, new LogNumber(_term, logIndex));
    }

    internal void FinishedWriting(int size, LogNumber logNumber)
    {
        _fileWriter.CompleteWrite(size, logNumber);
        _writtenLogIndex = logNumber.LogIndex;
        _committedLogIndex = logNumber.LogIndex;//TODO _committedLogIndex is should not be hear
    }

    internal void CancelWriting()
    {
        _fileWriter.CancelWrite();
    }

    internal FileReader GetFileReader(long logIndex)
    {
        return _logStore.GetFileReader(logIndex);
    }

    private void RemoveLogsAfter(long logIndex)
    {

    }

    private void RequestVote()
    {
    }

    private void TimeOut()
    {
        if (_nodeRole != NodeRoles.Leader)
        {
            Interlocked.Increment(ref _term);
            //TODO Voting
        }
    }

    internal void Flush()
    {
        _fileWriter.Flush();
        _fileWriter.Stop();
        //_walFileManager.Flush();
    }

    public void Dispose()
    {
        //_walFileManager.Dispose();
    }
}

using DistributedWAL.Storage;

namespace DistributedWAL;

// this is central point from where new log will be saved.
// it will contain information about logs and node status, if node is leader or not
// this is be central point. all the operations should go throgh this. write log, append log, read log etc
internal partial class Consensus : IConsensus
{
    private int _term;
    private long _writtenLogIndex = -1;
    private long _savedLogIndex => _writtenLogIndex; //TODO
    private long _committedLogIndex = -1;
    private int _nodeRole;

    private readonly ILogStore _logStore;
    private readonly IFileWriter _fileWriter;
    public int NodeRole { get { return Interlocked.CompareExchange(ref _nodeRole, 0, 0); } }
    public int Term => Interlocked.CompareExchange(ref _term, 0, 0);
    public long CommittedLogIndex => Interlocked.CompareExchange(ref _committedLogIndex, 0, 0);

    public Consensus(DistributedWalConfig config)
    {
        _logStore = new LogStore(config.MaxFileSize, config.WriteBatchTime, config.ReadBatchTime, config.LogDirectory);
        _fileWriter = _logStore.GetFileWriter();

        _term = 0;
        _nodeRole = NodeRoles.Leader; //TODO Not OK
    }

    public LogNumber WriteLog(ReadOnlySpan<byte> data)
    {
        if (NodeRole != NodeRoles.Leader)
            throw new DistributedWalException("Node is not a leader.");

        //TODO we should have some back pressure mechanism to max uncommited logs when follower are falling behind
        //TODO single producer, restrict concurrent calls
        var logIndex = Interlocked.Increment(ref _writtenLogIndex);
        var logNumber = new LogNumber(_term, logIndex);
        _fileWriter.WriteLog(data, logNumber);

        _writtenLogIndex = logNumber.LogIndex;
        _committedLogIndex = logNumber.LogIndex;//TODO _committedLogIndex is should not be hear
        return logNumber;
    }

    public IFileReader GetFileReader(long logIndex)
    {
        return _logStore.GetFileReader(logIndex);
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
}

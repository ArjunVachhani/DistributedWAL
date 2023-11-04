namespace DistributedWAL;

public class DistributedWalConfig
{
    private const int DefaultMaxFileSize = 64 * 1024 * 1024;//64MB
    private const int DefaultWriteBatchTime = 10;//10 Micro seconds;
    private const int DefaultReadBatchTime = 5;//5 Micro seconds;
    public string WalName { get; init; } = null!; //TODO nullable?
    public string LogDirectory { get; init; } = null!; //TODO nullable?
    public int MaxFileSize { get; init; } = DefaultMaxFileSize;
    public int WriteBatchTime { get; init; } = DefaultWriteBatchTime;
    public int ReadBatchTime { get; init; } = DefaultReadBatchTime;
}

public delegate void ResultCallback(LogNumber logNumber, object? result);

public class DistributedWal
{
    //creates a new wal with 1 node and becomes leader
    public static DistributedWal<T> DangerousCreateNewDistributedWal<T>(DistributedWalConfig config) where T : class, IStateMachine, new()
    {
        return new DistributedWal<T>(new Consensus(config));
    }

    public static DistributedWal<T> ResumeDistibutedWal<T>(DistributedWalConfig config) where T : class, IStateMachine, new()
    {
        throw new NotImplementedException();
    }
}

//distributedWAL should be singleton and it must start even if it is not able to connect to peers.
//peer discovery should be done at a later stage. if peer discover is done at a later stage and it is singleton then it allows it be easily configurable in dependecy injection
public class DistributedWal<T> where T : class, IStateMachine, new()
{
    private readonly Consensus _consensus;

    private readonly T _stateMachine; //TODO initialize

    private long _appliedLogIndex = -1; // TODO initialize

    private readonly Subscription WalApplierSubscription; //TODO this is hack, find proper api

    private ResultCallback? _resultCallback;

    public int NodeRole => _consensus.NodeRole;

    private bool _isStopping;
    private bool IsStopping => Volatile.Read(ref _isStopping);
    private readonly TaskCompletionSource StopTaskCompletionSource = new TaskCompletionSource();
    public Task StopAsync() //after calling stop new incoming logs should be rejected/discareded and should continue on smoother shutdown. already accpted log may be processed
    {
        Volatile.Write(ref _isStopping, true);
        return StopTaskCompletionSource.Task;
    }

    internal DistributedWal(Consensus consensus)
    {
        _consensus = consensus;
        _stateMachine = new T();
        WalApplierSubscription = AddSubscriber(LogProcessor, 0);
    }

    //TODO do we really need to expose this with public modifier?
    internal Subscription AddSubscriber(LogReaderAction logReader, long index)
    {
        return new Subscription(new WalReader(_consensus, logReader, index));
    }

    public Publication AddPublication()
    {
        return new Publication(new WalWriter(_consensus));
    }

    public void RegisterLogResultCallback(ResultCallback statusCallback)
    {
        _resultCallback = statusCallback;
    }

    public string? GetLeaderAddress()
    {
        return null;
    }

    //TODO I think there is no need to expose flush. it should expose stop method which will let host to shutdown it properly
    public void Flush()
    {
        _consensus.Flush();
    }

    //TODO this should not be public API. This should run in a separate thread.
    //TODO expose state machine worker thread status.
    public void ApplyCommittedLogs()
    {
        try
        {
            if (_consensus.CommittedLogIndex > _appliedLogIndex)
            {
                var logNumber = WalApplierSubscription.ProcessNext();
                if (logNumber == null)
                {
                    //TODO sleep/wait/semaphore
                }
            }
        }
        catch (Exception)
        {
            //TODO log and exist
            //Should not process remaining logs
            throw;
        }
    }

    //TODO find proper name
    private void LogProcessor(LogReader logReader)
    {
        if (1 < 1 + 1)//TODO non raft specific log will be passed
        {
            var result = _stateMachine.ApplyLog(logReader);

            try
            {
                _resultCallback?.Invoke(logReader.LogNumber, result);
            }
            catch (Exception)
            {
                //TODO Log error and ignore
            }
        }
        else
        {
            //TODO hanlde raft specific log such as create snapshot, membership changes etc
        }
    }

    public object? ExecuteReadOperation(object? command)
    {
        return _stateMachine.ExecuteReadOperation(command);
    }

    //should be lineralibity
    public object? ExecuteReadOperationLinearized(object? command)
    {
        return _stateMachine.ExecuteReadOperation(command);
    }

    //TODO Expose admin api for admin nodes/removing node/snapshot
}
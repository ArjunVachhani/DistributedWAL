namespace DistributedWAL;

internal class Consensus
{
    private int _term;
    private long _nextLogIndex;
    private bool _isLeader;

    internal LogWriter Copy(int term, long logIndex)
    {
        //TODO 
        if (_isLeader && _term < term)
        {
            Interlocked.Exchange(ref _term, term);
            _isLeader = false;
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

    /// <summary>
    /// Prefere <see cref="AppendFixedSizeLog(int)"/> because it allows multiple call to append log at same time
    /// </summary>
    /// <param name="maxSize"></param>
    /// <returns></returns>
    internal LogWriter AppendLog(int maxSize)
    {
        Interlocked.Increment(ref _nextLogIndex);
        return default;
    }

    internal void TimeOut()
    {
        if (!_isLeader)
        {
            Interlocked.Increment(ref _term);
            //TODO Voting
        }
    }
}

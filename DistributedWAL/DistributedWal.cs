using System.Net;

namespace DistributedWAL;

public class DistributedWalConfig
{
    public string WalName { get; set; } = null!; //TODO nullable?
    public string FilePath { get; set; } = null!; //TODO nullable?
}

public class DistributedWal
{
    private readonly WalFileManager _walFileManager = null!;
    private DistributedWal()
    {
    }

    //creates a new wal with 1 node and becomes leader
    public static DistributedWal DangerousCreateNewDistributedWal(DistributedWalConfig config)
    {
        throw new NotImplementedException();
    }

    public static DistributedWal ResumeDistibutedWal(DistributedWalConfig config)
    {
        throw new NotImplementedException();
    }

    public Task AppendLog(ReadOnlySpan<byte> data)
    {
        return Task.CompletedTask;
    }

    public Subscription AddSubscriber(LogReaderAction logReader, long index)
    {
        return new Subscription(logReader, index);
    }

    public Publication AddPublication(Action<long, int> statusCallback)
    {
        return new Publication(new WalWriter(_walFileManager));
    }

    public bool IsLeader => false;

    public IPAddress? GetLeaderAddress()
    {
        return null;
    }
}

public class Subscription
{
    private readonly LogReaderAction _logReader;
    private readonly long _index;
    internal Subscription(LogReaderAction logReader, long startIndex)
    {
        _logReader = logReader;
        _index = startIndex;
    }

    public long? ProcessNext()
    {
        _logReader(20, 123, new LogReader());
        return 20;
    }
}

public class Publication
{
    private readonly WalWriter _walWriter;
    internal Publication(WalWriter walWriter)
    {
        _walWriter = walWriter;
    }

    public bool IsLeader { get; set; }

    public LogWriter AppendFixedLengthLog(int length)
    {
        var logIndex = _walWriter.StartLog(length, true);
        return new LogWriter(_walWriter, logIndex);
    }

    public LogWriter AppendLog(int maxLength)
    {
        var logIndex = _walWriter.StartLog(maxLength, false);
        return new LogWriter(_walWriter, logIndex);
    }
}
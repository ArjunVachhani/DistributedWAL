namespace DistributedWAL;

public class Publication
{
    private readonly WalWriter _walWriter;
    internal Publication(WalWriter walWriter)
    {
        _walWriter = walWriter;
    }

    public int NodeRole => _walWriter.NodeRole;

    public LogWriter AppendFixedLengthLog(int length)
    {
        (var logIndex, var timeStamp) = _walWriter.StartLog(length, true);
        return new LogWriter(_walWriter, logIndex, timeStamp);
    }

    public LogWriter AppendLog(int maxLength)
    {
        (var logIndex, var timeStamp) = _walWriter.StartLog(maxLength, false);
        return new LogWriter(_walWriter, logIndex, timeStamp);
    }
}
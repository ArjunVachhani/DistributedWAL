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
        var logNumber = _walWriter.StartLog(length);
        return new LogWriter(_walWriter, logNumber);
    }
}
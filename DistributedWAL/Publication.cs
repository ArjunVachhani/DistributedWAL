namespace DistributedWAL;

public class Publication
{
    private readonly WalWriter _walWriter;
    internal Publication(WalWriter walWriter)
    {
        _walWriter = walWriter;
    }

    public int NodeRole => _walWriter.NodeRole;

    //TODO we will not have multiple producer, so we might not need fixed length api.
    //THINK ABOUT IT.
    public LogWriter AppendFixedLengthLog(int length)
    {
        (var logIndex, var term) = _walWriter.StartLog(length, true);
        return new LogWriter(_walWriter, logIndex, term);
    }

    public LogWriter AppendLog(int maxLength)
    {
        (var logIndex, var term) = _walWriter.StartLog(maxLength, false);
        return new LogWriter(_walWriter, logIndex, term);
    }
}
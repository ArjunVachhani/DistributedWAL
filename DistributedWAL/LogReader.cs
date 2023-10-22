namespace DistributedWAL;

public readonly ref struct LogReader
{
    private readonly WalReader _walReader;
    private readonly long _logIndex;
    private readonly int _term;

    public long LogIndex => _logIndex;
    public int Term => _term;

    [Obsolete("Invalid Constructor. Use LogWriter(WalWriter walWriter, long logIndex)", true)]
    public LogReader()
    {
        _walReader = null!;
    }

    internal LogReader(WalReader reader, int term, long logIndex)
    {
        _walReader = reader;
        _logIndex = logIndex;
        _term = term;
    }

    public int ReadInt32()
    {
        return _walReader.ReadInt32(_logIndex);
    }

    //TODO this is not really helpful api, need to think on how deserialize a json/custom binary deserialization. Probably exposing bytes.
    // also need to think about reading data that is not supposed to be read by logReader. think about safety concerns.
}
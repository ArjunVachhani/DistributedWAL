namespace DistributedWAL;

public readonly ref struct LogReader
{
    private readonly WalReader _walReader;
    private readonly LogNumber _logNumber;

    public LogNumber LogNumber => _logNumber;

    [Obsolete("Invalid Constructor. Use LogWriter(WalWriter walWriter, long logIndex)", true)]
    public LogReader()
    {
        _walReader = null!;
    }

    internal LogReader(WalReader reader, LogNumber logNumber)
    {
        _walReader = reader;
        _logNumber = logNumber;
    }

    public int ReadInt32()
    {
        return _walReader.ReadInt32(_logNumber.LogIndex);
    }

    public long ReadInt64()
    {
        return _walReader.ReadInt64(_logNumber.LogIndex);
    }

    public ReadOnlySpan<byte> GetSpan(int length)
    {
        return _walReader.GetSpan(_logNumber.LogIndex, length);
    }

    //TODO this is not really helpful api, need to think on how deserialize a json/custom binary deserialization. Probably exposing bytes.
    // also need to think about reading data that is not supposed to be read by logReader. think about safety concerns.
}
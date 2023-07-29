namespace DistributedWAL;

public readonly ref struct LogReader
{
    private readonly WalReader _walReader;
    internal LogReader(WalReader reader)
    {
        _walReader = reader;

    }

    public int ReadInt32()
    {
        return _walReader.ReadInt32();
    }
}
namespace DistributedWAL;

public readonly ref struct LogWriter
{
    private readonly WalWriter _walWriter;
    private readonly long _logIndex;

    [Obsolete("Invalid Constructor. Use LogWriter(WalWriter walWriter, long logIndex)", true)]
    public LogWriter()
    {
        _walWriter = null!;
    }

    internal LogWriter(WalWriter walWriter, long logIndex)
    {
        _walWriter = walWriter;
        _logIndex = logIndex;
    }

    public void Write(bool b)
    {
        _walWriter.Write(b, _logIndex);
    }

    public void Write(short s)
    {
        _walWriter.Write(s, _logIndex);
    }

    public void Write(int i)
    {
        _walWriter.Write(i, _logIndex);
    }

    public void Write(long l)
    {
        _walWriter.Write(l, _logIndex);
    }

    public void Write(decimal d)
    {
        _walWriter.Write(d, _logIndex);
    }

    public void Write(byte[] bytes, int offset, int count)
    {
        _walWriter.Write(bytes, offset, count, _logIndex);
    }

    public long FinishLog()
    {
        _walWriter.FinishLog();
        return 12;//return LSN
    }
}

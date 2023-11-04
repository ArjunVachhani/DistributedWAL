namespace DistributedWAL;

//TODO May be it should expose underlying span/to avoid copying it multiple time
public readonly ref struct LogWriter
{
    private readonly WalWriter _walWriter;
    private readonly LogNumber _logNumber;


    [Obsolete("Invalid Constructor. Use LogWriter(WalWriter walWriter, long logIndex)", true)]
    public LogWriter()
    {
        _walWriter = null!;
    }

    internal LogWriter(WalWriter walWriter, LogNumber logNumber)
    {
        _walWriter = walWriter;
        _logNumber = logNumber;
    }

    public void Write(bool b)
    {
        _walWriter.Write(b, _logNumber.LogIndex);
    }

    public void Write(short s)
    {
        _walWriter.Write(s, _logNumber.LogIndex);
    }

    public void Write(int i)
    {
        _walWriter.Write(i, _logNumber.LogIndex);
    }

    public void Write(long l)
    {
        _walWriter.Write(l, _logNumber.LogIndex);
    }

    public void Write(decimal d)
    {
        _walWriter.Write(d, _logNumber.LogIndex);
    }

    //TODO exposing alernative api underlying span can be faster
    //THINK ABOUT corruption possibility
    public void Write(Span<byte> bytes)
    {
        _walWriter.Write(bytes, _logNumber.LogIndex);
    }

    public LogNumber FinishLog()
    {
        _walWriter.FinishLog(_logNumber.LogIndex);
        return _logNumber;
    }

    public void DiscardLog()
    {
        _walWriter.DiscardLog(_logNumber.LogIndex);
    }
}

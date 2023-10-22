namespace DistributedWAL;

//TODO May be it should expose underlying span/to avoid copying it multiple time
public readonly ref struct LogWriter
{
    private readonly WalWriter _walWriter;
    private readonly long _logIndex;
    private readonly int _term;


    [Obsolete("Invalid Constructor. Use LogWriter(WalWriter walWriter, long logIndex)", true)]
    public LogWriter()
    {
        _walWriter = null!;
    }

    internal LogWriter(WalWriter walWriter, long logIndex, int term)
    {
        _walWriter = walWriter;
        _logIndex = logIndex;
        _term = term;
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

    //TODO exposing alernative api underlying span can be faster
    //THINK ABOUT corruption possibility
    public void Write(byte[] bytes, int offset, int count)
    {
        _walWriter.Write(bytes, offset, count, _logIndex);
    }

    public LogNumber FinishLog()
    {
        _walWriter.FinishLog(_logIndex);
        return new LogNumber(_term, _logIndex);
    }

    public void DiscardLog()
    {
        _walWriter.DiscardLog(_logIndex);
    }
}

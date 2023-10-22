namespace DistributedWAL;

//TODO think about ref struct
public readonly struct LogNumber
{
    [Obsolete("Use parameterized constructor.")]
    public LogNumber() { }

    public LogNumber(int term, long logIndex)
    {
        Term = term;
        LogIndex = logIndex;
    }

    public readonly int Term { get; init; }
    public readonly long LogIndex { get; init; }
}

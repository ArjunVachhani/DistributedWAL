namespace DistributedWAL.Storage;

internal readonly struct LogFile : IDisposable
{
    public int BytesWritten => LogFileStats.BytesWritten;
    public int BytesAvailableToRead => LogFileStats.BytesWritten - (int)FileStream.Position;
    public LogFileStats LogFileStats { get; private init; } //single LogFileStats is created per file.
    public int FileIndex { get; private init; }
    private FileStream FileStream { get; init; }

    public LogFile(LogFileStats logFileStats, int fileIndex, FileStream fileStream)
    {
        LogFileStats = logFileStats;
        FileIndex = fileIndex;
        FileStream = fileStream;
    }

    internal void Write(ReadOnlySpan<byte> bytes)
    {
        FileStream.Write(bytes);
    }

    internal void WaitForMoreData()
    {
        LogFileStats.WaitForMoreData();
    }

    internal int Read(Span<byte> bytes)
    {
        FileStream.ReadExactly(bytes);
        return bytes.Length;
    }

    internal void Flush()
    {
        FileStream.Flush();
        LogFileStats.SetBytesWritten((int)FileStream.Position);
    }

    public void Dispose()
    {
        FileStream.Dispose();
    }
}

internal class LogFileStats //TODO make singleton for a file
{
    private int _lockInt = 0;
    private readonly object _lock = new object();
    private int _bytesWritten;
    public int BytesWritten => Interlocked.CompareExchange(ref _bytesWritten, 0, 0);

    public LogFileStats(int bytesWritten)
    {
        _bytesWritten = bytesWritten;
    }

    internal void SetBytesWritten(int bytesWritten)
    {
        if (Interlocked.CompareExchange(ref _lockInt, 0, 0) > 1)
        {
            lock (_lock)
            {
                _bytesWritten = bytesWritten;
                _lockInt = 0;
                Monitor.PulseAll(_lock);
            }
        }
        else
            Interlocked.Exchange(ref _bytesWritten, bytesWritten);
    }

    internal void WaitForMoreData()
    {
        lock (_lock)
        {
            _lockInt++;
            Monitor.Wait(_lock, 100);
        }
    }
}
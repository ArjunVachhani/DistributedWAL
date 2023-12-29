namespace DistributedWAL.Storage;

internal interface IFileWriter : IDisposable
{
    LogNumber SavedLogNumber { get; }
    void WriteLog(ReadOnlySpan<byte> bytes, LogNumber logNumber);
    void Stop();
    void Flush();
}

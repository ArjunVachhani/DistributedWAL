namespace DistributedWAL.Storage;

internal interface IFileWriter
{
    void WriteLog(ReadOnlySpan<byte> bytes, LogNumber logNumber);
    void Stop();
    void Flush();
}

namespace DistributedWAL.Storage;

public interface IFileReader : IDisposable
{
    ReadOnlySpan<byte> ReadNextLog();
    void CompleteRead();
}

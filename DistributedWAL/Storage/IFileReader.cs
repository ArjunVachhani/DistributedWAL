namespace DistributedWAL.Storage;

internal interface IFileReader
{
    ReadOnlySpan<byte> ReadNextLog();
    void CompleteRead();
}

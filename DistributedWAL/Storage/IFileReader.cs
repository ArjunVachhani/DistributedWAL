namespace DistributedWAL.Storage;

internal interface IFileReader
{
    BufferSegment ReadNextLog();
    void CompleteRead();
}

namespace DistributedWAL.Storage;

internal interface IFileProvider
{
    LogFile GetNextFileForWrite(long logIndex);
    LogFile GetFileForRead(long logIndex);
}

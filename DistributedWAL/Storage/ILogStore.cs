namespace DistributedWAL.Storage;

internal interface ILogStore
{
    IFileWriter GetFileWriter();
    IFileReader GetFileReader(long logIndex);
}

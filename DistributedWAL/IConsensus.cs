using DistributedWAL.Storage;

namespace DistributedWAL;

internal interface IConsensus : IDisposable
{
    public int NodeRole { get; }
    public long CommittedLogIndex { get; }
    public int Term { get; }

    IFileReader GetFileReader(long logIndex);
    LogNumber WriteLog(ReadOnlySpan<byte> data);
}

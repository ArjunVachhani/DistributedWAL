using DistributedWAL.Storage;

namespace DistributedWAL;

public interface IConsensus : IDisposable
{
    public int NodeRole { get; }
    public long CommittedLogIndex { get; }
    public int Term { get; }
    public LogNumber SavedLogNumber { get; }
    public string? LeaderHost { get; }
    public string CurrentHostName { get; }

    public IFileReader GetFileReader(long logIndex);
    public LogNumber WriteLog(ReadOnlySpan<byte> data);
    public (int ResultCode, long LogIndex) AppendLog(string? hostName, int term, LogNumber prevLogNumber, ReadOnlySpan<byte> data);
    public (int ResultCode, long LogIndex) AppendLog(string? hostName, int term, long committedTerm);
    public void VoteGranted(int term);
    public bool RequestVote(int term, string host, LogNumber logNumber);
    public void UpdateTerm(int term);
}

namespace DistributedWAL.Networking;

internal class NetworkMessageType
{
    public const int RequestVote = 1;
    public const int RequestVoteResponse = 2;
    public const int AppendEntries = 3;
    public const int AppendEntriesResponse = 4;
    public const int InstallSnapshot = 5;
    public const int InstallSnapshotResponse = 6;
}

namespace DistributedWAL.Networking;

internal class AppendEntriesResultCode
{
    public const int Success = 0;
    public const int HigherTerm = -1;
    public const int MissingLog = -2;
}

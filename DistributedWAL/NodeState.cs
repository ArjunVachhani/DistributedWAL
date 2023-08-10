namespace DistributedWAL;

public class NodeInfo
{
    public int NodeRole { get; }
    public int Term { get; }
}

public static class NodeRoles
{
    public const int Follower = 0;
    public const int Candidate = 1;
    public const int Leader = 2;
}

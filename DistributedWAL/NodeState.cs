namespace DistributedWAL
{
    public class NodeInfo
    {
        public NodeRole Role { get; } = NodeRole.Leader;
        public int Term { get; }
    }


    public enum NodeRole
    {
        Follower = 1,
        Candidate = 2,
        Leader = 3,
    }
}

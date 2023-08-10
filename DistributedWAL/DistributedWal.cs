using System.Net;

namespace DistributedWAL;

public class DistributedWalConfig
{
    public string WalName { get; set; } = null!; //TODO nullable?
    public string FilePath { get; set; } = null!; //TODO nullable?
    public int MaxFileSize { get; set; }
}

public class DistributedWal
{
    private readonly Consensus _consensus;
    private DistributedWal(Consensus consensus)
    {
        _consensus = consensus;
    }

    //creates a new wal with 1 node and becomes leader
    public static DistributedWal DangerousCreateNewDistributedWal(DistributedWalConfig config)
    {
        return new DistributedWal(new Consensus(config));
    }

    public static DistributedWal ResumeDistibutedWal(DistributedWalConfig config)
    {
        throw new NotImplementedException();
    }

    public Subscription AddSubscriber(LogReaderAction logReader, long index)
    {
        return new Subscription(new WalReader( _consensus, logReader, index));
    }

    public Publication AddPublication(Action<long, int> statusCallback)
    {
        return new Publication(new WalWriter(_consensus));
    }

    public IPAddress? GetLeaderAddress()
    {
        return null;
    }
}
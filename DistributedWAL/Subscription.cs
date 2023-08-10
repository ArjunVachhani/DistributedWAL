namespace DistributedWAL;

public class Subscription
{
    private readonly WalReader _walReader;
    internal Subscription(WalReader walReader)
    {
        _walReader = walReader;
    }

    public long? ProcessNext()
    {
        return _walReader.ReadNextLog();
    }
}
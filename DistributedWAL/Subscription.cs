namespace DistributedWAL;

internal class Subscription
{
    private readonly WalReader _walReader;
    public Subscription(WalReader walReader)
    {
        _walReader = walReader;
    }

    public LogNumber? ProcessNext()
    {
        return _walReader.ReadNextLog();
    }
}
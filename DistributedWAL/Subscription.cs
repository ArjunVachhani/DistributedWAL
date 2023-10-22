namespace DistributedWAL;

internal class Subscription
{
    private readonly WalReader _walReader;
    internal Subscription(WalReader walReader)
    {
        _walReader = walReader;
    }

    internal LogNumber? ProcessNext()
    {
        return _walReader.ReadNextLog();
    }
}
namespace DistributedWAL;

internal class SystemClock : ISystemClock
{
    public long GetTimeStamp()
    {
        return (long)DateTime.UtcNow.Subtract(DateTime.UnixEpoch).TotalMilliseconds;
    }
}

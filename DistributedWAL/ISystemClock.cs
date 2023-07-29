namespace DistributedWAL;

internal interface ISystemClock
{
    long GetTimeStamp();
}

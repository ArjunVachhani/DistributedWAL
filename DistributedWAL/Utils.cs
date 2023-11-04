using System.Runtime.CompilerServices;

namespace DistributedWAL;

internal static class Utils
{
    static long epochTicks = DateTime.UnixEpoch.Ticks;
    const long ticksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetEpochDateDiffMicroseconds(DateTime dateTime)
    {
        return (epochTicks - dateTime.Ticks) / ticksPerMicrosecond;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetDateDiffMicroseconds(DateTime fromDateTime, DateTime toDateTime)
    {
        return (toDateTime.Ticks - fromDateTime.Ticks) / ticksPerMicrosecond;
    }
}

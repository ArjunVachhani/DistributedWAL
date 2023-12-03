using System.Buffers.Binary;
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static LogNumber ReadLogNumber(ReadOnlySpan<byte> buffer)
    {
        var term = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(Constants.TermOffset));
        var index = BinaryPrimitives.ReadInt64BigEndian(buffer.Slice(Constants.IndexOffset));
        return new LogNumber(term, index);
    }
}

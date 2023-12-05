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
    internal static LogNumber VerifyAndReadLogNumber(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < Constants.MessageOverhead)
            throw new DistributedWalException("Invalid Log size");

        var lengthStart = BinaryPrimitives.ReadInt32LittleEndian(buffer);
        var term = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(Constants.TermOffset));
        var index = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(Constants.IndexOffset));
        var lengthEnd = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(buffer.Length - Constants.MessageTrailerSize));

        if (lengthStart != lengthEnd || lengthStart + Constants.MessageOverhead != buffer.Length)
            throw new DistributedWalException("Log currupted. Length at start not equal to lenth at end.");

        return new LogNumber(term, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static LogNumber ReadLogNumber(ReadOnlySpan<byte> buffer)
    {
        var term = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(Constants.TermOffset));
        var index = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(Constants.IndexOffset));
        return new LogNumber(term, index);
    }
}

using System.IO.MemoryMappedFiles;

namespace DistributedWAL;

public delegate void LogReaderAction(long index, long timeStamp, LogReader arg2);

internal class WalReader
{
    internal void Read(LogReaderAction readerMethod)
    {
        var index = GetLogIndex();
        var timeStamp = GetTimeStamp(1);
        readerMethod(index, timeStamp, new LogReader(this));
    }

    internal int ReadInt32()
    {
        MemoryMappedViewAccessor viewAccessor = null!;

        return viewAccessor.ReadInt32(0);
    }

    internal long GetLogIndex()
    {
        return ReadLongUntillNonZero(0);
    }

    internal long GetTimeStamp(long position)
    {
        return ReadLongUntillNonZero(position);
    }

    private long ReadLongUntillNonZero(long position)
    {
        MemoryMappedViewAccessor viewAccessor = null!;
        long value = 0;
        do
        {
            value = viewAccessor.ReadInt64(position);
        } while (value == 0);
        return value;
    }
}

using System.IO.MemoryMappedFiles;

namespace DistributedWAL;

public delegate void LogReaderAction(LogReader logReader);

internal class WalReader
{
    private readonly Consensus _consensus;
    private readonly LogReaderAction _readerMethod;
    private long currentLogStartPosition = -1;
    private long currentLogIndex;
    private long currentLogSizeWithOverhead;
    private long currentPosition = 0;
    public WalReader(Consensus consensus, LogReaderAction readerMethod, long startLogIndex)
    {
        _consensus = consensus;
        _readerMethod = readerMethod;
        while (startLogIndex != ReadNextLog())
        { }
        currentLogIndex = startLogIndex;
    }

    internal long? ReadNextLog()
    {
        if (true)
        {
            currentLogStartPosition = currentLogStartPosition + currentLogSizeWithOverhead;
            currentLogSizeWithOverhead = ReadInt32();
            if(currentLogSizeWithOverhead == -1)//EOF end of file
            {
            }
            var term = ReadInt32();
            currentLogIndex = ReadInt64();
            currentPosition = 10;
            _readerMethod(new LogReader(this, term, currentLogIndex));
            return currentLogIndex;
        }
        //TODO return null
        //return null;
    }

    internal int ReadInt32(long logIndex)
    {
        if (currentLogIndex != logIndex && currentPosition + 8 > currentLogStartPosition + currentLogSizeWithOverhead)
            throw new DistributedWalException("Invalid logIndex");

        return ReadInt32();
    }

    private int ReadInt32()
    {
        MemoryMappedViewAccessor viewAccessor = null!;
        var value = viewAccessor.ReadInt32(currentPosition);
        currentPosition += 4;
        return value;
    }

    private long ReadInt64()
    {
        MemoryMappedViewAccessor viewAccessor = null!;
        var value = viewAccessor.ReadInt64(currentPosition);
        currentPosition += 8;
        return value;
    }
}

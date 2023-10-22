using System.IO.MemoryMappedFiles;

namespace DistributedWAL;

public delegate void LogReaderAction(LogReader logReader);

internal class WalReader
{
    private readonly Consensus _consensus;
    private readonly LogReaderAction _readerMethod;
    private long currentLogStartPosition = 0;
    private long currentLogIndex = -1;
    private long currentLogSizeWithOverhead = 0;
    private long currentPosition = 0;
    public WalReader(Consensus consensus, LogReaderAction readerMethod, long startLogIndex)
    {
        _consensus = consensus;
        _readerMethod = readerMethod;
        //if (_consensus.CommittedLogIndex > -1)
        //{
        //    //ideally it should not have to seek it should be handled outside.
        //    while (startLogIndex != ReadNextLog()) 
        //    { }
        //    currentLogIndex = startLogIndex;
        //}
    }

    internal LogNumber? ReadNextLog()
    {
        if (currentLogIndex < _consensus.CommittedLogIndex)
        {
            currentLogStartPosition = currentLogStartPosition + currentLogSizeWithOverhead;
            currentPosition = currentLogStartPosition;
            var currentLogSize = ReadInt32();
            currentLogSizeWithOverhead = currentLogSize + Constants.MessageOverhead;
            if (currentLogSize == -1)//EOF end of file
            {
                //TODO
            }
            var term = ReadInt32();
            currentLogIndex = ReadInt64();
            _readerMethod(new LogReader(this, term, currentLogIndex));
            return new LogNumber(term, currentLogIndex);
        }
        return null;
    }

    internal int ReadInt32(long logIndex)
    {
        if (currentLogIndex != logIndex && currentPosition + 8 > currentLogStartPosition + currentLogSizeWithOverhead)
            throw new DistributedWalException("Invalid logIndex");

        return ReadInt32();
    }

    private int ReadInt32()
    {
        MemoryMappedViewAccessor viewAccessor = _consensus.WalFileManager.WalFile.ReadOnlyViewAccessor;
        var value = viewAccessor.ReadInt32(currentPosition);
        currentPosition += 4;
        return value;
    }

    private long ReadInt64()
    {
        MemoryMappedViewAccessor viewAccessor = _consensus.WalFileManager.WalFile.ReadOnlyViewAccessor;
        var value = viewAccessor.ReadInt64(currentPosition);
        currentPosition += 8;
        return value;
    }
}

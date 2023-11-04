using DistributedWAL.Storage;

namespace DistributedWAL;

public delegate void LogReaderAction(LogReader logReader);

internal class WalReader
{
    private readonly Consensus _consensus;
    private readonly FileReader _fileReader;
    private readonly LogReaderAction _readerMethod;

    private BufferSegment _bufferSegment;
    private int _currentLogTerm = -1;
    private long _currentLogIndex = -1;
    private int _currentLogSizeWithOverhead = 0;
    private int _currentPosition = 0;
    public WalReader(Consensus consensus, LogReaderAction readerMethod, long startLogIndex)
    {
        _consensus = consensus;
        _readerMethod = readerMethod;
        _fileReader = _consensus.GetFileReader(startLogIndex);
    }

    internal LogNumber? ReadNextLog()
    {
        if (_currentLogIndex < _consensus.CommittedLogIndex)
        {
            _bufferSegment = _fileReader.ReadNextLog();
            _currentPosition = 0;
            _currentLogSizeWithOverhead = ReadInt32() + Constants.MessageOverhead;
            _currentLogTerm = ReadInt32();
            _currentLogIndex = ReadInt64();
            _readerMethod(new LogReader(this, new LogNumber(_currentLogTerm, _currentLogIndex)));
            _fileReader.CompleteRead(_currentLogSizeWithOverhead);
            return new LogNumber(_currentLogTerm, _currentLogIndex);
        }
        return null;
    }

    internal int ReadInt32(long logIndex)
    {
        if (_currentLogIndex != logIndex && _currentPosition + 4 > _currentLogSizeWithOverhead)
            throw new DistributedWalException("Invalid logIndex");

        return ReadInt32();
    }

    internal long ReadInt64(long logIndex)
    {
        if (_currentLogIndex != logIndex && _currentPosition + 8 > _currentLogSizeWithOverhead)
            throw new DistributedWalException("Invalid logIndex");

        return ReadInt64();
    }

    internal ReadOnlySpan<byte> GetSpan(long logIndex, int length)
    {
        if (_currentLogIndex != logIndex && _currentPosition + length > _currentLogSizeWithOverhead)
            throw new DistributedWalException("Invalid logIndex");

        return GetSpan(length);
    }

    private int ReadInt32()
    {
        var value = _bufferSegment.ReadInt32(_currentPosition);
        _currentPosition += 4;
        return value;
    }

    private long ReadInt64()
    {
        var value = _bufferSegment.ReadInt64(_currentPosition);
        _currentPosition += 8;
        return value;
    }

    private ReadOnlySpan<byte> GetSpan(int length)
    {
        var span = _bufferSegment.GetSpan(_currentPosition, length);
        _currentPosition += length;
        return span;
    }
}

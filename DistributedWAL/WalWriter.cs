using System.IO.MemoryMappedFiles;

namespace DistributedWAL;

//responsibility : tldr avoid data corruption. Responsibility of WalWriter is to restrict writing outside of the allocated space and publication can write at a time one log to avoid over writing previous log
internal class WalWriter
{

    private readonly Consensus _consensus;

    private MemoryMappedViewAccessor? _mappedViewAccessor;
    private long _logIndex = -1;
    private int _startPosition = -1;
    private int _currentPosition = -1;
    private int _maxPosition = -1;
    private bool _isFixedSize = true;

    public int NodeRole => _consensus.NodeRole;

    internal WalWriter(Consensus consensus)
    {
        _consensus = consensus;
    }

    internal void Write(bool b, long logIndex)
    {
        if (logIndex != _logIndex || _maxPosition < _currentPosition + 1)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(b);
    }

    internal void Write(short s, long logIndex)
    {
        if (logIndex != _logIndex || _maxPosition < _currentPosition + 2)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(s);
    }

    internal void Write(int i, long logIndex)
    {
        if (logIndex != _logIndex || _maxPosition < _currentPosition + 4)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(i);
    }

    internal void Write(long l, long logIndex)
    {
        if (logIndex != _logIndex || _maxPosition < _currentPosition + 8)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(l);
    }

    internal void Write(decimal d, long logIndex)
    {
        if (logIndex != _logIndex || _maxPosition < _currentPosition + 16)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(d);
    }

    internal void Write(byte[] bytes, int offset, int count, long logIndex)
    {
        if (logIndex != _logIndex || _maxPosition < _currentPosition + 16)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(bytes, offset, count);
    }

    internal (long logIndex, int term) StartLog(int maxLength, bool isFixedSize)
    {
        if (_logIndex != -1)
            throw new DistributedWalException("WalWriter is in middle of writing a log. Can not do StartLog");

        (_mappedViewAccessor, var term, _logIndex, _startPosition) = _consensus.RequestWriteSegment(maxLength + Constants.MessageOverhead, isFixedSize);
        _currentPosition = _startPosition;
        _maxPosition = checked(_currentPosition + maxLength + Constants.MessageOverhead);
        _isFixedSize = isFixedSize;
        WriteInternal(maxLength);
        WriteInternal(term);
        WriteInternal(_logIndex);
        return (_logIndex, term);
    }

    internal void FinishLog(long logIndex)
    {
        if (logIndex != _logIndex || _logIndex == -1)
            throw new DistributedWalException("WalWriter is not writing a log.");

        int messageSize = ((_isFixedSize ? _maxPosition : _currentPosition) - _startPosition) - Constants.MessageOverhead;

        _currentPosition = checked(_startPosition + messageSize + Constants.MessageHeaderSize);
        WriteInternal(messageSize);
        var endPosition = _currentPosition - 1;

        if (!_isFixedSize)
        {
            _currentPosition = _startPosition;
            WriteInternal(messageSize);
        }
        _consensus.FinishedWriting(_startPosition, endPosition, _logIndex);
        _mappedViewAccessor = null;
        _logIndex = -1;
        _startPosition = -1;
        _currentPosition = -1;
        _maxPosition = -1;
    }

    internal void DiscardLog(long logIndex)
    {
        if (logIndex != _logIndex || _logIndex == -1)
            throw new DistributedWalException("WalWriter is not writing a log.");
    }

    private void WriteInternal(bool b)
    {
        _mappedViewAccessor!.Write(_currentPosition, b);
        _currentPosition += 1;
    }

    private void WriteInternal(short s)
    {
        _mappedViewAccessor!.Write(_currentPosition, s);
        _currentPosition += 2;
    }

    private void WriteInternal(int i)
    {
        _mappedViewAccessor!.Write(_currentPosition, i);
        _currentPosition += 4;
    }

    private void WriteInternal(long l)
    {
        _mappedViewAccessor!.Write(_currentPosition, l);
        _currentPosition += 8;
    }

    private void WriteInternal(decimal d)
    {
        _mappedViewAccessor!.Write(_currentPosition, d);
        _currentPosition += 16;
    }

    private void WriteInternal(byte[] bytes, int offset, int count)
    {
        _mappedViewAccessor!.WriteArray(_currentPosition, bytes, offset, count);
        _currentPosition += 16;
    }
}

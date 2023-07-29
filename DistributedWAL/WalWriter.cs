using System.IO.MemoryMappedFiles;

namespace DistributedWAL;

internal class WalWriter
{
    private readonly WalFileManager _fileManager;
    internal WalWriter(WalFileManager fileManager)
    {
        _fileManager = fileManager;
    }

    private MemoryMappedViewAccessor? _mappedViewAccessor;
    private long _logIndex = -1;
    private int _startPosition = -1;
    private int _currentPosition = -1;
    private int _maxPosition = -1;
    private bool _isFixedSize = true;

    //Message layout
    //[4 length] + [8 timestamp] + [8 logIndex] + [4 term] + [X message] +  [4 length]
    //So 1 byte message will take 29 bytes to save, 10 bytes message will take 38 bytes to save. and so on.

    public const int messageHeaderSize = 24; //[4 payload size] + [8 timestamp] + [8 logIndex] + [4 term]
    public const int messageTrailerSize = 4; //[4 payload size]
    private const int messageOverhead = messageHeaderSize + messageTrailerSize;

    public void Write(long logIndex, long timestamp, ReadOnlySpan<byte> data)
    {
        var mmf = MemoryMappedFile.CreateFromFile("");
        var stream = mmf.CreateViewStream();

        Span<byte> bytes = stackalloc byte[8];

        BitConverter.TryWriteBytes(bytes, logIndex);
        stream.Write(bytes);

        BitConverter.TryWriteBytes(bytes, timestamp);
        stream.Write(bytes);

        BitConverter.TryWriteBytes(bytes, data.Length);
        stream.Write(bytes.Slice(0, 2));

        stream.Write(data);
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

    internal long StartLog(int maxLength, bool isFixedSize)
    {
        if (_logIndex != -1)
            throw new DistributedWalException("WalWriter is in middle of writing a log. Can not do StartLog");

        (_mappedViewAccessor, _logIndex, _startPosition, var timestamp) = _fileManager.RequestWriteSegment(maxLength + messageOverhead, isFixedSize);
        _currentPosition = _startPosition;
        _maxPosition = checked(_currentPosition + maxLength + messageOverhead);
        _isFixedSize = isFixedSize;
        var term = 0;//TODO Term
        WriteInternal(maxLength);
        WriteInternal(timestamp);
        WriteInternal(_logIndex);
        WriteInternal(term);
        return _logIndex;
    }

    internal void FinishLog()
    {
        if (_logIndex == -1)
            throw new DistributedWalException("WalWriter is not writing a log.");

        int messageSize;
        if (_isFixedSize)
            messageSize = (_maxPosition - _startPosition) - messageOverhead;
        else
            messageSize = (_currentPosition - _startPosition) - messageHeaderSize;

        _currentPosition = checked(_startPosition + messageSize + messageHeaderSize);
        WriteInternal(messageSize);
        var endPosition = _currentPosition - 1;

        if (!_isFixedSize)
        {
            _currentPosition = _startPosition;
            WriteInternal(messageSize);
        }
        _fileManager.FinishedWriting(_startPosition, endPosition, _logIndex);
        _mappedViewAccessor = null;
        _logIndex = -1;
        _startPosition = -1;
        _currentPosition = -1;
        _maxPosition = -1;
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

using DistributedWAL.Storage;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace DistributedWAL;

//responsibility : tldr avoid data corruption. Responsibility of WalWriter is to restrict writing outside of the allocated space and publication can write at a time one log to avoid over writing previous log
internal class WalWriter
{
    private readonly Consensus _consensus;

    private BufferSegment _bufferSegment;
    private LogNumber _logNumber = new LogNumber(-1, -1);
    private int _currentPosition = -1;
    private int _maxPosition = -1;

    public int NodeRole => _consensus.NodeRole;

    internal WalWriter(Consensus consensus)
    {
        _consensus = consensus;
    }

    internal void Write(bool b, long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _maxPosition < _currentPosition + 1)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(b);
    }

    internal void Write(short s, long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _maxPosition < _currentPosition + 2)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(s);
    }

    internal void Write(int i, long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _maxPosition < _currentPosition + 4)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(i);
    }

    internal void Write(long l, long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _maxPosition < _currentPosition + 8)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(l);
    }

    internal void Write(decimal d, long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _maxPosition < _currentPosition + 16)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(d);
    }

    internal void Write(ReadOnlySpan<byte> bytes, long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _maxPosition < _currentPosition + bytes.Length)
            throw new DistributedWalException("Log storage is full.");

        WriteInternal(bytes);
    }

    internal LogNumber StartLog(int maxLength)
    {
        if (_logNumber.LogIndex != -1)
            throw new DistributedWalException("WalWriter is in middle of writing a log. Can not do StartLog");

        (_bufferSegment, _logNumber) = _consensus.RequestWriteSegment(maxLength + Constants.MessageOverhead);
        _currentPosition = 4;// skip first four bytes for payload size
        _maxPosition = maxLength + Constants.MessageHeaderSize;
        WriteInternal(_logNumber.Term);
        WriteInternal(_logNumber.LogIndex);
        return _logNumber;
    }

    internal void FinishLog(long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _logNumber.LogIndex == -1)
            throw new DistributedWalException("WalWriter is not writing a log.");

        var messageSize = _currentPosition - Constants.MessageHeaderSize;

        WriteInternal(messageSize);
        _currentPosition = 0;
        WriteInternal(messageSize);
        _consensus.FinishedWriting(messageSize + Constants.MessageOverhead, _logNumber);
        _logNumber = new LogNumber(-1, -1);
        _currentPosition = -1;
        _maxPosition = -1;
    }

    internal void DiscardLog(long logIndex)
    {
        if (logIndex != _logNumber.LogIndex || _logNumber.LogIndex == -1)
            throw new DistributedWalException("WalWriter is not writing a log.");

        _consensus.CancelWriting();
        _logNumber = new LogNumber(-1, -1);
        _currentPosition = -1;
        _maxPosition = -1;
    }

    private void WriteInternal(bool b)
    {
        _bufferSegment[_currentPosition] = Convert.ToByte(b);
        _currentPosition += 1;
    }

    private void WriteInternal(short s)
    {
        BinaryPrimitives.WriteInt16BigEndian(_bufferSegment.GetSpan(_currentPosition, 2), s);
        _currentPosition += 2;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteInternal(int i)
    {
        _bufferSegment.Write(_currentPosition, i);
        _currentPosition += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteInternal(long l)
    {
        _bufferSegment.Write(_currentPosition, l);
        _currentPosition += 8;
    }

    private void WriteInternal(decimal d)
    {
        Span<int> bytes = stackalloc int[4];
        decimal.GetBits(d, bytes);
        BinaryPrimitives.WriteInt32BigEndian(_bufferSegment.GetSpan(_currentPosition, 4), bytes[0]);
        BinaryPrimitives.WriteInt32BigEndian(_bufferSegment.GetSpan(_currentPosition + 4, 4), bytes[1]);
        BinaryPrimitives.WriteInt32BigEndian(_bufferSegment.GetSpan(_currentPosition + 8, 4), bytes[2]);
        BinaryPrimitives.WriteInt32BigEndian(_bufferSegment.GetSpan(_currentPosition + 12, 4), bytes[3]);
        _currentPosition += 16;
    }

    private void WriteInternal(ReadOnlySpan<byte> bytes)
    {
        bytes.CopyTo(_bufferSegment.GetSpan(_currentPosition, bytes.Length));
        _currentPosition += bytes.Length;
    }
}

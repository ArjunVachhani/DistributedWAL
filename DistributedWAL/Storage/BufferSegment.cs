using System.Runtime.CompilerServices;

namespace DistributedWAL.Storage;

internal readonly struct BufferSegment
{
    private readonly byte[] _buffer;
    private readonly int _startPosition;
    private readonly int _length;

    public int Length => _length;

    [Obsolete("Use parameterized constructor.", true)]
    public BufferSegment() { _buffer = null!; _startPosition = 0; _length = 0; }

    public BufferSegment(byte[] bytes, int startPosition, int length)
    {
        _buffer = bytes;
        _startPosition = startPosition;
        _length = length;
    }

    public byte this[int index]
    {
        get
        {
            if (index < 0 || index > _length)
                throw new IndexOutOfRangeException();

            return _buffer[_startPosition + index];
        }
        set
        {
            if (index < 0 || index > _length)
                throw new IndexOutOfRangeException();

            _buffer[_startPosition + index] = value;
        }
    }

    public Span<byte> GetSpan(int index, int length)
    {
        if (index < 0 || index + length > _length)
            throw new IndexOutOfRangeException();

        return _buffer.AsSpan(_startPosition + index, length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(int index, int value)
    {
        if (index < 0 || index + 4 > _length)
            throw new IndexOutOfRangeException();

        index = index + _startPosition;
        _buffer[index] = (byte)value;
        _buffer[index + 1] = (byte)(value >> 8);
        _buffer[index + 2] = (byte)(value >> 16);
        _buffer[index + 3] = (byte)(value >> 24);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(int index, long value)
    {
        if (index < 0 || index + 8 > _length)
            throw new IndexOutOfRangeException();

        index = index + _startPosition;
        _buffer[index] = (byte)value;
        _buffer[index + 1] = (byte)(value >> 8);
        _buffer[index + 2] = (byte)(value >> 16);
        _buffer[index + 3] = (byte)(value >> 24);
        _buffer[index + 4] = (byte)(value >> 32);
        _buffer[index + 5] = (byte)(value >> 40);
        _buffer[index + 6] = (byte)(value >> 48);
        _buffer[index + 7] = (byte)(value >> 56);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt32(int index)
    {
        if (index < 0 || index + 4 > _length)
            throw new IndexOutOfRangeException();

        index = index + _startPosition;
        return BitConverter.ToInt32(_buffer, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadInt64(int index)
    {
        if (index < 0 || index + 8 > _length)
            throw new IndexOutOfRangeException();

        index = index + _startPosition;
        return BitConverter.ToInt64(_buffer, index);
    }
}

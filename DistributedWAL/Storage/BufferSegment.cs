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

    public Span<byte> GetSpan(int index, int length)
    {
        if (index < 0 || index + length > _length)
            throw new IndexOutOfRangeException();

        return _buffer.AsSpan(_startPosition + index, length);
    }
}

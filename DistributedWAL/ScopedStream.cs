namespace DistributedWAL;

//TODO implementation
//we need to have scoped stream to avoid data corruption by mistake.
internal class ScopedStream : Stream
{
    private readonly Stream _stream;
    public ScopedStream(Stream stream, bool canRead, bool canSeek, bool canWrite, bool length, bool startPosition)
    {
        _stream = stream;
    }

    public override bool CanRead { get; }
    public override bool CanSeek { get; }
    public override bool CanWrite { get; }
    public override long Length { get; }
    public override long Position { get; set; }

    public override void Flush()
    {
        throw new NotImplementedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotImplementedException();
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }
}

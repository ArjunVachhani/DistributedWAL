using System.Buffers.Binary;

namespace DistributedWAL.Storage;

//single producer, single consumer forward only buffer(non ring);
//why single producer : because we need to support cancel write, which will release previous space. and we dont want hole
//why we need complete read : because we are not copying data. once read complete is then only it is safe to allow it to be written
//non ring buffer. why? we are using buffers because we want to reduce write calls. if it is ring buffer it might be possible that message starts at end and continue in the bigning of actual byte[] buffer which defeats the purpose of reducing write calls
//why non blocking producer? so that, producer can buffer the logs until consumer is writing to disk
//want continuous logs so that filestream write call can be reduced

internal class LogBuffer
{
    private const int DefaultBufferSize = 65536;

    private readonly ManualResetEventSlim _readManualRestEventSlim = new ManualResetEventSlim(false);
    private readonly ManualResetEventSlim _writeManualRestEventSlim = new ManualResetEventSlim(false);
    private readonly byte[] _bytes;

    private LogNumber _logNumber = default!;

    private int _writeInProgress = 0;

    private int _writerPosition = 0;

    //_readerPosition must be advanced when data for a given segment is read by reader and Ok for writer to overwrite it
    private int _readerPosition = 0;

    private int _lock = 0;

    public LogBuffer() : this(DefaultBufferSize) { }

    public LogBuffer(int bufferSize)
    {
        _bytes = new byte[bufferSize];
    }

    public int Capacity => _bytes.Length;

    public bool WaitForDataToRead()
    {
        _readManualRestEventSlim.Reset();
        return _readManualRestEventSlim.Wait(100);
    }

    public bool WaitForDataToWrite()
    {
        _writeManualRestEventSlim.Reset();
        return _writeManualRestEventSlim.Wait(100);
    }

    public LogBufferReadResult BeginRead()
    {
        int writerPosition;
        int readerPositoin;
        LogNumber logNumber;
        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }
        writerPosition = _writerPosition;
        readerPositoin = _readerPosition;
        logNumber = _logNumber;
        Interlocked.Exchange(ref _lock, 0);
        return new LogBufferReadResult(new ReadOnlySpan<byte>(_bytes, readerPositoin, writerPosition - readerPositoin), logNumber);
    }

    public bool TryReadLog(out BufferSegment bufferSegment)
    {
        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }
        if (_readerPosition < _writerPosition)
        {
            var size = BitConverter.ToInt32(_bytes, _readerPosition);
            bufferSegment = new BufferSegment(_bytes, _readerPosition, size + Constants.MessageOverhead);
            Interlocked.Exchange(ref _lock, 0);
            return true;
        }
        else
        {
            Interlocked.Exchange(ref _lock, 0);
            bufferSegment = default;
            return false;
        }
    }

    public void CompleteRead(int size)
    {
        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }

        Interlocked.Add(ref _readerPosition, size);
        if (_readerPosition == _writerPosition && _writeInProgress == 0) //reset pointers to begining.
        {
            Interlocked.Exchange(ref _writerPosition, 0);
            Interlocked.Exchange(ref _readerPosition, 0);
            if (!_writeManualRestEventSlim.IsSet)
                _writeManualRestEventSlim.Set();
        }

        Interlocked.Exchange(ref _lock, 0);
    }

    public int GetAvailableBytesToRead()
    {
        int size;
        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }
        size = _writerPosition - _readerPosition;
        Interlocked.Exchange(ref _lock, 0);
        return size;
    }

    public int GetAvailableBytesToWrite()
    {
        int size;
        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }
        size = _bytes.Length - _writerPosition;
        Interlocked.Exchange(ref _lock, 0);
        return size;
    }

    public bool TryWrite(ReadOnlySpan<byte> data, LogNumber logNumber)
    {
        if (data.Length < 1 || data.Length > _bytes.Length)
            throw new DistributedWalException($"Invalid size {data.Length}. Size must be in rage the of 1 to {_bytes.Length}");

        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }

        bool result = false;
        var messageSize = data.Length + Constants.MessageOverhead;
        if (_writerPosition + messageSize <= _bytes.Length)
        {
            var startPosition = _writerPosition;
            result = true;
            _writeInProgress = 1;
            _logNumber = logNumber;
            Interlocked.Exchange(ref _lock, 0);

            var span = _bytes.AsSpan(startPosition, messageSize);
            BinaryPrimitives.WriteInt32LittleEndian(span, data.Length);
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(Constants.TermOffset), logNumber.Term);
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(Constants.IndexOffset), logNumber.LogIndex);
            data.CopyTo(span.Slice(Constants.MessageHeaderSize));
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(Constants.MessageHeaderSize + data.Length), data.Length);

            while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
            {
                Thread.SpinWait(1);
            }
            _writeInProgress = 0;
            _writerPosition += messageSize;
        }

        Interlocked.Exchange(ref _lock, 0);

        if (result && !_readManualRestEventSlim.IsSet)
            _readManualRestEventSlim.Set();

        return result;
    }

    public bool TryBeginWrite(int size, out BufferSegment bufferSegment)
    {
        if (size < 1 || size > _bytes.Length)
            throw new DistributedWalException($"Invalid size {size}. Size must be in rage the of 1 to {_bytes.Length}");

        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }

        if (_writerPosition + size > _bytes.Length)
        {
            bufferSegment = default;
            PoorTelemetry.WaitingForReader++;
            Interlocked.Exchange(ref _lock, 0);
            return false;
        }
        else
        {
            bufferSegment = new BufferSegment(_bytes, _writerPosition, size);
            Interlocked.Exchange(ref _writeInProgress, 1);
            Interlocked.Exchange(ref _lock, 0);
            return true;
        }
    }

    public void CompleteWrite(int size, LogNumber logNumber)
    {
        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }

        if (_writeInProgress == 1)
        {
            Interlocked.Add(ref _writerPosition, size);
            _logNumber = logNumber;
            Interlocked.Exchange(ref _writeInProgress, 0);
        }

        Interlocked.Exchange(ref _lock, 0);

        if (!_readManualRestEventSlim.IsSet)
            _readManualRestEventSlim.Set();
    }

    //why we need cancel : suppose we got half log and we want to rollback. for example serializer failed or network connection failed.
    public void CancelWrite()
    {
        while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
        {
            Thread.SpinWait(1);
        }

        Interlocked.Exchange(ref _writeInProgress, 0);

        Interlocked.Exchange(ref _lock, 0);
    }
}

internal readonly ref struct LogBufferReadResult
{
    [Obsolete("Use parameterized constructor.", true)]
    public LogBufferReadResult() { }

    public LogBufferReadResult(ReadOnlySpan<byte> bytes, LogNumber logNumber)
    {
        Bytes = bytes;
        LogNumber = logNumber;
    }
    internal ReadOnlySpan<byte> Bytes { get; private init; }
    internal LogNumber LogNumber { get; private init; }
}
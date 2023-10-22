namespace DistributedWAL;

//single producer, single consumer ring buffer with gaps. 


//single producer, single consumer forward only buffer(non ring);
//why single producer : because we need to support cancel write, which will release previous space. and we dont want hole
//why we need complete read : because we are not copying data. once read is then only it is safe to allow it to be
//non ring buffer. why? we are using buffers because we want to reduce write calls. if it is ring buffer it might be possible that message starts at end and continue in the bigning of actual byte[] buffer which defeats the purpose of reducing write calls

// goal
// want continuous logs so that filestream write call can be reduced
// what non blocking producer so, producer can buffer the logs until consumer is writing to disk
// expose API to provide next continuous block-size.
// consumer should be able to ask no of bytes to read.


internal class LogBuffer
{
    private const int DefaultBufferSize = 65536;

    private readonly byte[] _bytes;

    private LogNumber _logNumber = default!;

    private int _writeInProgress = 0;

    private int _writerPosition = 0;

    //_readerPosition must be advanced when data for a given segment is read by reader and Ok for writer to overwrite it
    private int _readerPosition = 0;

    private int _writerLock = 0;
    private int _readerLock = 0;

    private bool WriteInProgress => Interlocked.CompareExchange(ref _writeInProgress, 0, 0) == 1;
    private int WriterPosition => Interlocked.CompareExchange(ref _writerPosition, 0, 0);
    private int ReaderPosition => Interlocked.CompareExchange(ref _readerPosition, 0, 0);

    public LogBuffer() : this(DefaultBufferSize) { }

    public LogBuffer(int bufferSize)
    {
        _bytes = new byte[bufferSize];
    }

    public LogBufferReadResult BeginRead()
    {
        int writerPosition;
        int readerPositoin;
        LogNumber logNumber;
        while (Interlocked.CompareExchange(ref _writerLock, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }
        while (Interlocked.CompareExchange(ref _readerLock, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }
        writerPosition = WriterPosition;
        readerPositoin = ReaderPosition;
        logNumber = _logNumber;
        Interlocked.Exchange(ref _readerLock, 0);
        Interlocked.Exchange(ref _writerLock, 0);
        return new LogBufferReadResult(new ReadOnlySpan<byte>(_bytes, readerPositoin, writerPosition), logNumber);
    }

    public void CompleteRead(int size)
    {
        while (Interlocked.CompareExchange(ref _readerLock, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }
        Interlocked.Add(ref _readerPosition, size);
        Interlocked.Exchange(ref _readerLock, 0);
    }

    public int GetSize()
    {
        int size;
        while (Interlocked.CompareExchange(ref _writerLock, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }
        while (Interlocked.CompareExchange(ref _readerLock, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }
        size = WriterPosition - ReaderPosition;
        Interlocked.Exchange(ref _readerLock, 0);
        Interlocked.Exchange(ref _writerLock, 0);
        return size;
    }

    public Span<byte> BeginWrite(int size)
    {
        if (size < 1 || size > 65536)
            throw new DistributedWalException($"Invalid size {size}. Size must be in rage the of 1 to 65536");

        while (WriteInProgress)
        {
            Thread.Yield();
        }

        Span<byte> span = default;
        do
        {
            while (Interlocked.CompareExchange(ref _writerLock, 1, 0) == 1)
            {
                Thread.SpinWait(10);
            }

            if (ReaderPosition == WriterPosition) //reset pointers to begining.
            {
                while (Interlocked.CompareExchange(ref _readerLock, 1, 0) == 1)
                {
                    Thread.SpinWait(10);
                }
                Interlocked.Exchange(ref _writerPosition, 0);
                Interlocked.Exchange(ref _readerPosition, 0);
                Interlocked.Exchange(ref _readerLock, 0);
            }

            if (WriterPosition + size > _bytes.Length)
            {
                Interlocked.Exchange(ref _writerLock, 0);
                Thread.SpinWait(20);
            }
            else
            {
                span = new Span<byte>(_bytes, WriterPosition, size);
                Interlocked.Exchange(ref _writeInProgress, 1);
                Interlocked.Exchange(ref _writerLock, 0);
            }
        } while (WriterPosition + size > _bytes.Length);
        return span;
    }

    public void CompleteWrite(int size, LogNumber logNumber)
    {
        while (Interlocked.CompareExchange(ref _writerLock, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }

        if (WriteInProgress)
        {
            Interlocked.Add(ref _writerPosition, size);
            _logNumber = logNumber;
            Interlocked.Exchange(ref _writeInProgress, 0);
        }

        Interlocked.Exchange(ref _writerLock, 0);
    }

    //why we need cancel : suppose we got half log and we want to rollback for example serializer failed or network connection failed
    public void CancelWrite()
    {
        while (Interlocked.CompareExchange(ref _writerLock, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }

        if (WriteInProgress)
        {
            Interlocked.Exchange(ref _writeInProgress, 0);
        }

        Interlocked.Exchange(ref _writerLock, 0);
    }

    //why we need clear : on leader election we want to reset non committed logs
    //may be it should have wait for some time(few seconds) if write in progress then cancel
    public void Clear()
    {

    }
}

internal ref struct LogBufferReadResult
{
    public LogBufferReadResult() { }
    public LogBufferReadResult(ReadOnlySpan<byte> bytes, LogNumber logNumber)
    {
        Bytes = bytes;
        LogNumber = logNumber;
    }
    internal ReadOnlySpan<byte> Bytes { get; private init; }
    internal LogNumber LogNumber { get; private init; }
}
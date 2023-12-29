namespace DistributedWAL.Storage;

internal class FileWriter : IFileWriter
{
    const int BatchSize = 4096;//TODO use batch size/page size

    private readonly IFileProvider _fileProvider;
    private readonly LogBuffer _logBuffer = new LogBuffer();
    private readonly int _writeBatchTimeInMicroseconds;
    private readonly Thread _writerThread;

    private LogFile _logFile;
    private bool stopping = false;//TODO better mechanism
    //private bool writesInProgress = false;

    public LogNumber SavedLogNumber { get; }//TODO

    public FileWriter(IFileProvider fileprovider, int writeBatchTime)
    {
        _fileProvider = fileprovider;
        _logFile = fileprovider.GetNextFileForWrite(0);
        _writeBatchTimeInMicroseconds = writeBatchTime;
        _writerThread = new Thread(WriterToFile);
        _writerThread.Start();
    }

    public void WriteLog(ReadOnlySpan<byte> bytes, LogNumber logNumber)
    {
        if (bytes.Length < 1 || bytes.Length > _logBuffer.Capacity)
            throw new DistributedWalException($"Invalid size {bytes.Length}. Size must be in rage the of 1 to {_logBuffer.Capacity}");

        while (!_logBuffer.TryWrite(bytes, logNumber))
        {
            Thread.SpinWait(20);
        }
    }

    public BufferSegment RequestWriteSegment(int size)
    {
        if (size < 1 || size > _logBuffer.Capacity)
            throw new DistributedWalException($"Invalid size {size}. Size must be in rage the of 1 to {_logBuffer.Capacity}");

        BufferSegment bufferSegment;
        while (!_logBuffer.TryBeginWrite(size, out bufferSegment))
        {
            Thread.SpinWait(20);
        }
        return bufferSegment;
    }

    public void CompleteWrite(int size)
    {
        _logBuffer.CompleteWrite(size);
    }

    public void CancelWrite()
    {
        _logBuffer.CancelWrite();
    }

    public void RemoveLogsFrom(LogNumber logNumber)
    {
        Flush();
        //TODO remove logs from current file and all previous files if needed
    }

    public void Stop()
    {
        Volatile.Write(ref stopping, true);
        Flush();
    }

    public void Flush()
    {
        while (_logBuffer.GetAvailableBytesToRead() > 0)
        {
            Thread.SpinWait(5);
        }
    }

    private void WriterToFile()
    {
        DateTime batchStartTime = DateTime.UtcNow;
        bool batchStarted = false;
        int bytes;
        while (Volatile.Read(ref stopping) == false || batchStarted)//TODO  || Volatile.Read(ref writesInProgress)
        {
            bytes = _logBuffer.GetAvailableBytesToRead();
            if (bytes > 0 && batchStarted == false)
            {
                batchStartTime = DateTime.UtcNow;
                batchStarted = true;
            }

            var microseconds = Utils.GetDateDiffMicroseconds(batchStartTime, DateTime.UtcNow);
            if (bytes > 0 && bytes < BatchSize && microseconds < _writeBatchTimeInMicroseconds)
            {
                PoorTelemetry.ReaderWaitingForMoreData++;
                Thread.SpinWait(25);
            }
            else if (bytes > BatchSize || (batchStarted && microseconds >= _writeBatchTimeInMicroseconds))
            {
                var data = _logBuffer.BeginRead();
                //TODO new file if file max size is reached.
                _logFile.Write(data);
                _logBuffer.CompleteRead(data.Length);
                PoorTelemetry.BytesWritten += data.Length;
                _logFile.Flush();
                batchStarted = false;
                PoorTelemetry.FileWriteCount++;
            }
            else if (bytes == 0)
            {
                if (_logBuffer.WaitForDataToRead())//we got some data. we want loop to continue even if stop is received. because we have to save data.
                {
                    batchStarted = true;
                    batchStartTime = DateTime.UtcNow;
                }
            }
        }
    }

    public void Dispose()
    {
        //TODO
        throw new NotImplementedException();
    }
}

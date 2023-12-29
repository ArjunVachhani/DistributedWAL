using System.Buffers;

namespace DistributedWAL.Storage;

internal class FileReader : IFileReader
{
    const int BatchSize = 4096;//TODO use batch size/page size

    private readonly IFileProvider _fileProvider;
    private readonly LogBuffer _logBuffer = new LogBuffer();
    private readonly int _readBatchTimeInMicroseconds;
    private readonly Thread _readerThread;

    private LogFile _logFile;
    private bool stopping = false;//TODO better mechanism
    private int _lastLogSize = 0;
    public FileReader(IFileProvider fileProvider, LogFile logFile, int readBatchTime)
    {
        _fileProvider = fileProvider;
        _logFile = logFile;
        _readBatchTimeInMicroseconds = readBatchTime;
        _readerThread = new Thread(WriteToLogBuffer);
        _readerThread.Start();
    }

    public ReadOnlySpan<byte> ReadNextLog()
    {
        ReadOnlySpan<byte> bytes;
        while (!_logBuffer.TryReadLog(out bytes))
        {
            _logBuffer.WaitForDataToRead();
        }
        _lastLogSize = bytes.Length;
        //TODO we will need to have logic to check it log was overwritten to discard the cache and re read the logs
        return bytes;
    }

    public void CompleteRead()
    {
        if (_lastLogSize > 0)
        {
            _logBuffer.CompleteRead(_lastLogSize);
            _lastLogSize = 0;
        }
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

    private void WriteToLogBuffer()
    {
        DateTime batchStartTime = DateTime.UtcNow;
        byte[] tempBuffer = new byte[_logBuffer.Capacity];
        int tempBufferStart = 0;
        bool batchStarted = false;
        int bytesAvailable;
        while (Volatile.Read(ref stopping) == false || batchStarted)//TODO  || Volatile.Read(ref writesInProgress)
        {
            bytesAvailable = _logFile.BytesAvailableToRead;
            if (bytesAvailable > 0 && batchStarted == false)
            {
                batchStartTime = DateTime.UtcNow;
                batchStarted = true;
            }
            //TODO we need to jump to new file on reaching end of file.
            var microseconds = Utils.GetDateDiffMicroseconds(batchStartTime, DateTime.UtcNow);
            if (bytesAvailable > 0 && bytesAvailable < BatchSize && microseconds < _readBatchTimeInMicroseconds)
            {
                PoorTelemetry.ReaderWaitingForMoreData++;
                Thread.SpinWait(25);
            }
            else if (bytesAvailable > BatchSize || (batchStarted && microseconds >= _readBatchTimeInMicroseconds))
            {
                var bufferCapacity = tempBuffer.Length - tempBufferStart;
                var idealReadSize = bufferCapacity > 16384 ? 16384 : bufferCapacity;
                var readSize = bytesAvailable > idealReadSize ? idealReadSize : bytesAvailable;
                var bytesToWrite = tempBufferStart + _logFile.Read(tempBuffer.AsSpan(tempBufferStart, readSize));
                tempBufferStart = 0;//next time it should start from 0, if data is left it will be shifted and tempBufferStart will be updated
                var startPosition = 0;
                PoorTelemetry.BytesRead += readSize;
                PoorTelemetry.FileReadCount++;
                while (bytesToWrite > 4)
                {
                    var logSize = BitConverter.ToInt32(tempBuffer, startPosition) + Constants.MessageOverhead;
                    if (bytesToWrite >= logSize)
                    {
                        WriteLog(tempBuffer, startPosition, logSize);
                        bytesToWrite -= logSize;
                        startPosition += logSize;
                    }
                    else
                    {
                        ShiftDataInBegning(tempBuffer, startPosition, bytesToWrite);
                        tempBufferStart = bytesToWrite;
                        bytesToWrite = 0;// so that if outside of while is not executed.
                    }
                }
                if (bytesToWrite > 0)
                {
                    ShiftDataInBegningFast(tempBuffer, startPosition, bytesToWrite);
                    tempBufferStart = bytesToWrite;
                }
                batchStarted = false;
            }
            else if (bytesAvailable == 0)
            {
                _logFile.WaitForMoreData();
            }
        }
    }

    private void WriteLog(byte[] bytes, int startIndex, int length)
    {
        BufferSegment bufferSegment;
        while (!_logBuffer.TryBeginWrite(length, out bufferSegment))
        {
            _logBuffer.WaitForDataToWrite();
            //Thread.SpinWait(20);//TODO we are busy spining, may be we should wait
        }
        bytes.AsSpan(startIndex, length).CopyTo(bufferSegment.GetSpan(0, length));
        _logBuffer.CompleteWrite(length);
    }

    private void ShiftDataInBegning(byte[] bytes, int startIndex, int length)
    {
        var temp = ArrayPool<byte>.Shared.Rent(length);
        Array.Copy(bytes, startIndex, temp, 0, length);
        Array.Copy(temp, 0, bytes, 0, length);
        ArrayPool<byte>.Shared.Return(bytes);
        PoorTelemetry.BytesCopied += length;
        PoorTelemetry.TimesCopied++;
    }

    private void ShiftDataInBegningFast(byte[] bytes, int startIndex, int length)
    {
        PoorTelemetry.BytesCopiedFast += length;
        PoorTelemetry.TimesCopiedFast++;
        for (int i = 0; i < length; i++)
        {
            bytes[i] = bytes[startIndex + i];
        }
    }

    public void Dispose()
    {
        //TODO
        throw new NotImplementedException();
    }
}

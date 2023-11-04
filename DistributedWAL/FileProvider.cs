using DistributedWAL.Storage;
using System.Diagnostics.Tracing;
using System.Text.RegularExpressions;
using System.Threading.Channels;

namespace DistributedWAL;

internal class FileProvider
{
    private static readonly Regex fileNameRegex = new Regex("logs-\\d*\\.bin", RegexOptions.Compiled);

    private readonly FileIndex _fileIndex = new FileIndex();
    private readonly ChannelReader<(FileStream FileStream, int FileIndex, long? FirstLogIndex)> _channelReader;//channel stores empty and last partially filled file(partially filled file will be available on startup)
    private readonly ChannelWriter<(FileStream FileStream, int FileIndex, long? FirstLogIndex)> _channelWriter;
    private readonly CancellationTokenSource StopCts = new CancellationTokenSource();
    private readonly string _folderPath;
    private readonly int _logFileMaxSize;

    private FileStream? _lockFileStream;
    private volatile int _lock = 0;
    private int _logFileNextIndex = 0;
    private LogFile? _currentLogFile;
    public FileProvider(int logFileMaxSize, string folderPath)//TODO out param for last logIndex and lastTerm
    {
        _logFileMaxSize = logFileMaxSize;
        _folderPath = folderPath;
        var channelOption = new BoundedChannelOptions(3) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = false };
        var channel = Channel.CreateBounded<(FileStream FileStream, int FileIndex, long? FirstLogIndex)>(channelOption);
        _channelReader = channel.Reader;
        _channelWriter = channel.Writer;

        Initialize();
    }

    private void Initialize()
    {
        AcquireLock();
        GetAllLogFiles();
        TruncateHalfWrittenLog();

        if (_fileIndex.Count > 0)
            _logFileNextIndex = _fileIndex.GetLast()!.Value.FileIndex + 1;

        OpenLastFileForWrite();
        _ = SpareFileCreator(); // Do not await
    }

    private void AcquireLock()
    {
        if (!Directory.Exists(_folderPath))
            Directory.CreateDirectory(_folderPath);

        _lockFileStream = new FileStream(Path.Combine(_folderPath, "lock.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    }

    private void GetAllLogFiles()
    {
        Span<byte> buffer = stackalloc byte[8];
        foreach (var file in Directory.GetFiles(_folderPath))
        {
            if (!fileNameRegex.IsMatch(file))
                continue;

            var fileIndex = int.Parse(file.Substring(file.IndexOf('-') + 1, file.IndexOf('.') - (file.IndexOf('-') + 1)));
            var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            if (fileStream.Length > Constants.MessageOverhead)
            {
                fileStream.ReadExactly(buffer);//read size and term.
                var size = BitConverter.ToInt32(buffer);
                if (size > 0)
                {
                    fileStream.ReadExactly(buffer);
                    var logIndex = BitConverter.ToInt64(buffer);
                    _fileIndex.Add(new FileEntry(fileIndex, logIndex));
                    fileStream.Dispose();
                }
                else
                {
                    fileStream.Dispose();
                    File.Delete(Path.Combine(_folderPath, file));
                }
            }
            else
            {
                fileStream.Dispose();
                File.Delete(Path.Combine(_folderPath, file));
            }
        }
        _fileIndex.Sort();
    }

    private void TruncateHalfWrittenLog()
    {
        if (_fileIndex.Count > 0)
        {
            var fileIndex = _fileIndex.GetLast()!.Value.FileIndex;
            using var fileStream = OpenFile(fileIndex, FileAccess.ReadWrite);
            FileLogHelper.TruncateUnfinishedLog(fileStream);
            if (fileStream.Position == 0)
            {
                fileStream.Dispose();
                File.Delete(FilePath(fileIndex));
            }
        }
    }

    private void OpenLastFileForWrite()
    {
        if (_fileIndex.Count > 0)
        {
            var fileEntry = _fileIndex.GetLast()!.Value;
            var fileIndex = fileEntry.FileIndex;
            var logIndex = fileEntry.FirstLogIndex;
            var fileStream = OpenFile(fileIndex, FileAccess.ReadWrite);
            if (FileLogHelper.SeekToEndForWrite(fileStream))//if last file is not written fully put it in channel
            {
                _channelWriter.TryWrite((fileStream, fileIndex, logIndex));
                _fileIndex.Remove(fileEntry);
            }
        }
    }

    internal LogFile GetNextFileForWrite(long logIndex)
    {
        try
        {
            //lock
            while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
            {
                Thread.SpinWait(2);
            }

            (FileStream FileStream, int FileIndex, long? FirstLogIndex) fileDetail;
            while (!_channelReader.TryRead(out fileDetail))
            {
                Thread.SpinWait(2);
            }

            var fileIndexLogIndex = new FileEntry(fileDetail.FileIndex, fileDetail.FirstLogIndex ?? logIndex);
            _fileIndex.Add(fileIndexLogIndex);
            _currentLogFile = new LogFile(new LogFileStats((int)fileDetail.FileStream.Position), fileDetail.FileIndex, fileDetail.FileStream);
        }
        finally
        {
            Interlocked.Exchange(ref _lock, 0);
        }
        return _currentLogFile.Value;
    }

    internal void Stop()
    {
        StopCts.Cancel();
        _lockFileStream?.Dispose();
    }

    private async Task SpareFileCreator()
    {
        while (await _channelWriter.WaitToWriteAsync(StopCts.Token))
        {
            var fileStream = OpenFile(_logFileNextIndex, FileAccess.ReadWrite);
            fileStream.SetLength(_logFileMaxSize);
            await _channelWriter.WriteAsync((fileStream, _logFileNextIndex, null));
            _logFileNextIndex++;
        }
    }

    internal LogFile GetFileForRead(long logIndex)
    {
        int? fileIndex = null;
        try
        {
            while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
            {
                Thread.SpinWait(2);
            }

            fileIndex = _fileIndex.FindFile(logIndex)?.FileIndex;
        }
        finally
        {
            Interlocked.Exchange(ref _lock, 0);
        }

        if (fileIndex != null)
        {
            var fileStream = OpenFile(fileIndex.Value, FileAccess.Read);
            FileLogHelper.SeekToLog(fileStream, logIndex);
            fileStream.Flush(true);
            if (_currentLogFile?.FileIndex == fileIndex)
                return new LogFile(_currentLogFile?.LogFileStats!, fileIndex.Value, fileStream);
            else
                return new LogFile(new LogFileStats((int)fileStream.Length), fileIndex.Value, fileStream);
        }
        else
        {
            //TODO file for that log not found. 
            throw new DistributedWalException("file for given logIndex not found.");
        }
    }

    internal LogFile GetFileForRead(int fileIndex)
    {
        if (File.Exists(FilePath(fileIndex)))
        {
            var fileStream = OpenFile(fileIndex, FileAccess.Read);
            if (_currentLogFile?.FileIndex == fileIndex)
                return new LogFile(_currentLogFile?.LogFileStats!, fileIndex, fileStream);
            else
                return new LogFile(new LogFileStats((int)fileStream.Length), fileIndex, fileStream);
        }
        else
        {
            //TODO file does not exists
            throw new DistributedWalException("file for given logIndex not found.");
        }
    }

    private FileStream OpenFile(int fileIndex, FileAccess fileAccess)
    {
        if (fileAccess == FileAccess.Read)
            return new FileStream(FilePath(fileIndex), FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 1);//buffering causes issue of stale read. disabled buffering with bufferSize = 1
        else
            return new FileStream(FilePath(fileIndex), FileMode.OpenOrCreate, fileAccess, FileShare.Read);
    }

    private string FilePath(int fileIndex)
    {
        return Path.Combine(_folderPath, $"logs-{fileIndex}.bin");
    }
}

using System.IO.MemoryMappedFiles;
using System.Text;

namespace DistributedWAL;

internal class WalFileManager : IDisposable
{
    private readonly int _maxFileSize;
    private readonly NodeInfo _nodeState;
    private readonly ISystemClock _systemClock;

    private int _requestWriteLockInt = 0;

    private int _inProgressWrites = 0;
    private int _isUnkownSizeInProgress = 0; //0 == false, 1 == true

    private int _nextPosition = 0;
    private long _nextLogIndex = 0;

    private int _writtenPosition = 0;
    private long _writtenLogIndex = 0;

    private string _folderPath;
    private int _fileIndex = 0;
    private readonly WalFile _walFile;

    internal WalFileManager(NodeInfo nodeState, int maxSize, ISystemClock systemClock, string folderPath)
    {
        _nodeState = nodeState;
        _maxFileSize = maxSize;
        _systemClock = systemClock;

        _folderPath = Path.HasExtension(folderPath) ? Path.GetDirectoryName(folderPath)! : folderPath;
        
        if (!Directory.Exists(_folderPath))
            Directory.CreateDirectory(_folderPath);

        foreach (var file in Directory.GetFiles(_folderPath))
        {
            if (!file.StartsWith("log"))
                continue;

            var fileIndex = int.Parse(file.Substring(file.IndexOf('-') + 1, file.IndexOf('.') - (file.IndexOf('-') + 1)));
            if (fileIndex > _fileIndex)
                _fileIndex = fileIndex;
        }

        EnsureFileExists(_fileIndex);
        (var lastLogIndex, var lastLogTerm, var lastLogEndPosition) = GetLogIndexAndTruncatePartiallyWrittenLog(_fileIndex);
        if (lastLogIndex == null && _fileIndex >= 1)
        {
            (lastLogIndex, lastLogTerm, lastLogEndPosition) = GetLogIndexAndTruncatePartiallyWrittenLog(_fileIndex - 1);
            if (lastLogIndex != null)
                _fileIndex = _fileIndex - 1;
        }
        if (lastLogIndex == null && _fileIndex >= 1)
            throw new DistributedWalException($"Corrupted file index {_fileIndex - 1}.");

        _nextLogIndex = (lastLogIndex ?? -1) + 1;
        _nextPosition = (lastLogEndPosition ?? -1) + 1;
        _writtenLogIndex = _nextLogIndex - 1;
        _writtenPosition = _nextPosition - 1;
        _walFile = new WalFile(Path.Combine(_folderPath, $"log-{_fileIndex}.bin"));
    }

    internal (MemoryMappedViewAccessor viewAccessor, long logIndex, int position, long timeStamp) RequestWriteSegment(int length, bool isFixedSize)
    {
        if (length <= 0)
            throw new DistributedWalException("Length must be positive");

        if (_nodeState.Role != NodeRole.Leader)
            throw new DistributedWalException("Node is not a leader.");

        while (Interlocked.CompareExchange(ref _requestWriteLockInt, 1, 0) == 1)
        {
            Thread.SpinWait(10);
        }
        Interlocked.Increment(ref _inProgressWrites);

        //Wait if unknown size write in progress
        while (Interlocked.CompareExchange(ref _isUnkownSizeInProgress, 1, 1) == 1)
        {
            Thread.Sleep(10);
        }

        if (!isFixedSize)
        {
            while (Interlocked.CompareExchange(ref _isUnkownSizeInProgress, 1, 0) == 1)
            {
                Thread.SpinWait(10);
            }
        }

        if (_nextPosition + length > _maxFileSize)
        {
            _walFile.ReadWriteViewAccessor.Flush();
            //TODO new file
        }

        int position = _nextPosition;//make a local copy to avoid reading next value
        long logIndex = _nextLogIndex;

        _nextPosition += length;
        ++_nextLogIndex;

        Interlocked.Exchange(ref _requestWriteLockInt, 0);
        return (_walFile.ReadWriteViewAccessor, logIndex, position, _systemClock.GetTimeStamp());
    }

    internal void FinishedWriting(int startPosition, int endPosition, long logIndex)
    {
        while (Interlocked.CompareExchange(ref _writtenPosition, endPosition, startPosition - 1) != startPosition - 1)
        {
            Thread.SpinWait(10);
        }

        while (Interlocked.CompareExchange(ref _writtenLogIndex, logIndex, logIndex - 1) != logIndex - 1)
        {
            Thread.SpinWait(10);
        }
        Interlocked.Decrement(ref _inProgressWrites);
    }

    public void Dispose()
    {
        _walFile.ReadWriteViewAccessor.Flush();
        _walFile.Dispose();
    }

    private void EnsureFileExists(int index)
    {
        var latestFile = Path.Combine(_folderPath, $"log-{index}.bin");
        if (!File.Exists(latestFile))
        {
            using var stream = File.Create(latestFile);
            stream.SetLength(_maxFileSize);
        }
    }

    private FileStream OpenFile(int index)
    {
        var latestFile = Path.Combine(_folderPath, $"log-{index}.bin");
        FileStream fileStream;
        if (!File.Exists(latestFile))
            fileStream = File.Create(latestFile);
        else
            fileStream = File.Open(latestFile, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        return fileStream;
    }

    private (long? logIndex, int? term, int? lastLogEndPosition) GetLogIndexAndTruncatePartiallyWrittenLog(int index)
    {
        using var fileStream = OpenFile(index);
        long? lastLogIndex = null;
        int? lastLogterm = null;
        int? lastLogEndPosition = null;
        using BinaryReader reader = new BinaryReader(fileStream, Encoding.UTF8, true);
        while (fileStream.Position <= fileStream.Length - 28)
        {
            var lengthAtStart = reader.ReadInt32();
            _ = reader.ReadInt64();
            var logIndex = reader.ReadInt64();
            var logTerm = reader.ReadInt32();
            if (fileStream.Position + lengthAtStart > fileStream.Length)
                throw new DistributedWalException($"Corrupted file {fileStream.Name}.");
            fileStream.Seek(lengthAtStart, SeekOrigin.Current);
            var lengthAtEnd = reader.ReadInt32();
            if (lengthAtStart == lengthAtEnd && lengthAtStart != 0)
            {
                lastLogIndex = logIndex;
                lastLogterm = logTerm;
                lastLogEndPosition = (int)fileStream.Position - 1;
            }
            else if (lengthAtStart == 0)
            {
                break;
            }
            else
            {
                fileStream.Seek(-(lengthAtStart + 28), SeekOrigin.Current);
                using BinaryWriter writer = new BinaryWriter(fileStream, Encoding.UTF8, true);
                while (fileStream.Position < fileStream.Length - 8)
                    writer.Write(0L);
            }
        }
        return (lastLogIndex, lastLogterm, lastLogEndPosition);
    }
}
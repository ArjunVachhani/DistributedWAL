using System.Text.RegularExpressions;
using System.Threading.Channels;

namespace DistributedWAL;

internal class FileProvider
{
    private static readonly Regex fileNameRegex = new Regex("logs-\\d*\\.bin", RegexOptions.Compiled);
    private readonly List<FileIndexLogIndex> _index = new List<FileIndexLogIndex>();
    private readonly ChannelReader<(FileStream FileStream, int FileIndex, long? FirstLogIndex)> _channelReader;//channel stores empty and last partially filled file(partially filled file will be available on startup)
    private readonly ChannelWriter<(FileStream FileStream, int FileIndex, long? FirstLogIndex)> _channelWriter;
    private volatile int _lock = 0;

    private int _logFileNextIndex = 0;
    private readonly string _folderPath;
    private readonly int _logFileMaxSize;
    public FileProvider(int logFileMaxSize, string folderPath)
    {
        _logFileMaxSize = logFileMaxSize;
        _folderPath = folderPath;
        var channelOption = new BoundedChannelOptions(3) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = false };
        var channel = Channel.CreateBounded<(FileStream FileStream, int FileIndex, long? FirstLogIndex)>(channelOption);
        _channelReader = channel.Reader;
        _channelWriter = channel.Writer;
    }

    private void Initialize()
    {
        if (!Directory.Exists(_folderPath))
            Directory.CreateDirectory(_folderPath);

        byte[] buffer = new byte[8];
        foreach (var file in Directory.GetFiles(_folderPath))
        {
            if (!fileNameRegex.IsMatch(file))
                continue;

            var fileIndex = int.Parse(file.Substring(file.IndexOf('-') + 1, file.IndexOf('.') - (file.IndexOf('-') + 1)));
            var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            if (fileStream.Length > 20)
            {
                fileStream.ReadExactly(buffer);//read size and term.
                var size = BitConverter.ToInt32(buffer);
                if (size > 0)
                {
                    if (fileIndex >= _logFileNextIndex)
                        _logFileNextIndex = fileIndex + 1;

                    fileStream.ReadExactly(buffer);
                    var logIndex = BitConverter.ToInt64(buffer);
                    _index.Add(new FileIndexLogIndex(fileIndex, logIndex));
                    fileStream.Dispose();
                }
                else
                {
                    //empty log file. may be we can delete it
                }
            }
            else
            {
                //empty/invalid log file. may be we can delete it.
            }
        }

        //TODO sort _index
        //TODO find last file and create stats. check for logs trucate partial log
    }

    internal FileStream GetNextFileForWrite(long logIndex)
    {
        try
        {
            //lock
            while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
            {
                Thread.SpinWait(10);
            }

            if (!_channelReader.TryRead(out var fileDetail))
            {
                Thread.SpinWait(10);
            }

            var fileIndexLogIndex = new FileIndexLogIndex(fileDetail.FileIndex, fileDetail.FirstLogIndex ?? logIndex);
            _index.Add(fileIndexLogIndex);
            return fileDetail.FileStream;
        }
        finally
        {
            Interlocked.Exchange(ref _lock, 0);
        }
    }


    private CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();
    internal void Stop()
    {
        CancellationTokenSource.Cancel();
    }

    private async Task SpareFileCreator()
    {
        while (await _channelWriter.WaitToWriteAsync(CancellationTokenSource.Token))
        {
            var fileStream = new FileStream($"logs-{_logFileNextIndex}.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            fileStream.SetLength(_logFileMaxSize);
            await _channelWriter.WriteAsync((fileStream, _logFileNextIndex, null));
            _logFileNextIndex++;
        }
    }

    internal FileStream GetFileForRead(long logIndex)
    {
        try
        {
            while (Interlocked.CompareExchange(ref _lock, 1, 0) == 1)
            {
                Thread.SpinWait(10);
            }

            var fileIndex = FindFileIndex(logIndex);

            return null!;
        }
        finally
        {
            Interlocked.Exchange(ref _lock, 0);
        }
    }

    private int FindFileIndex(long firstLogIndex)
    {
        //TODO
        return 0;
    }

    readonly struct FileIndexLogIndex
    {
        [Obsolete("Use parameterized constructor.")]
        public FileIndexLogIndex() { }

        public FileIndexLogIndex(int fileIndex, long firstLogIndex)
        {
            FileIndex = fileIndex;
            FirstLogIndex = firstLogIndex;
        }

        public readonly int FileIndex { get; init; }
        public readonly long FirstLogIndex { get; init; }
    }
}

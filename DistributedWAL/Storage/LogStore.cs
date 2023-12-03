namespace DistributedWAL.Storage;

internal class LogStore : ILogStore
{
    private readonly IFileProvider _fileProvider;
    private readonly IFileWriter _fileWriter;
    private readonly int _readBatchTime;
    public LogStore(int logFileMaxSize, int writeBatchTime, int readBatchTime, string folderPath)
    {
        _fileProvider = new FileProvider(logFileMaxSize, folderPath);
        _fileWriter = new FileWriter(_fileProvider, writeBatchTime);
        _readBatchTime = readBatchTime;
    }

    public IFileWriter GetFileWriter()
    {
        return _fileWriter;
    }

    public IFileReader GetFileReader(long logIndex)
    {
        return new FileReader(_fileProvider, _fileProvider.GetFileForRead(logIndex), _readBatchTime);
    }
}

namespace DistributedWAL.Storage;

internal class LogStore
{
    private readonly FileProvider _fileProvider;
    private readonly FileWriter _fileWriter;
    private readonly int _readBatchTime;
    public LogStore(int logFileMaxSize, int writeBatchTime, int readBatchTime, string folderPath)
    {
        _fileProvider = new FileProvider(logFileMaxSize, folderPath);
        _fileWriter = new FileWriter(_fileProvider, writeBatchTime);
        _readBatchTime = readBatchTime;
    }

    public FileWriter GetFileWriter()
    {
        return _fileWriter;
    }

    public FileReader GetFileReader(long logIndex)
    {
        return new FileReader(_fileProvider, _fileProvider.GetFileForRead(logIndex), _readBatchTime);
    }
}

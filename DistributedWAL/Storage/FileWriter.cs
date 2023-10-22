namespace DistributedWAL.Storage;

internal class FileWriter
{
    private readonly FileProvider _fileProvider;
    private readonly LogBuffer _logBuffer;
    private FileStream _fileStream;
    public FileWriter(FileProvider fileprovider)
    {
        _fileProvider = fileprovider;
        _fileStream = fileprovider.GetNextFileForWrite(0);
        _logBuffer = new LogBuffer();
    }
}

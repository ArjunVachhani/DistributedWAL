using DistributedWAL.Storage;

namespace DistributedWAL;

public delegate void LogReaderAction(LogNumber logNumber, ReadOnlySpan<byte> bytes);

internal class Subscription
{
    private readonly IConsensus _consensus;
    private readonly IFileReader _fileReader;
    private readonly LogReaderAction _readerMethod;

    private LogNumber _currentLogNumber = new LogNumber(0, -1);
    
    public Subscription(IConsensus consensus, LogReaderAction readerMethod, long startLogIndex)
    {
        _consensus = consensus;
        _readerMethod = readerMethod;
        _fileReader = _consensus.GetFileReader(startLogIndex);
    }

    public LogNumber? ProcessNext()
    {
        if (_currentLogNumber.LogIndex < _consensus.CommittedLogIndex)
        {
            var bytes = _fileReader.ReadNextLog();
            _currentLogNumber = Utils.VerifyAndReadLogNumber(bytes);
            var messageBytes = bytes.Slice(Constants.MessageHeaderSize, bytes.Length - Constants.MessageOverhead);
            _readerMethod(_currentLogNumber, messageBytes);
            _fileReader.CompleteRead();
            return _currentLogNumber;
        }
        return null;
    }
}
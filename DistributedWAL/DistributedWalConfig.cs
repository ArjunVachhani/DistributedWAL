namespace DistributedWAL;

public class DistributedWalConfig
{
    private const int DefaultPort = 6543;
    private const int DefaultMaxFileSize = 64 * 1024 * 1024;//64MB
    private const int DefaultWriteBatchTime = 10;//10 Micro seconds;
    private const int DefaultReadBatchTime = 5;//5 Micro seconds;

    public int Port { get; init; } = DefaultPort;
    public string WalName { get; init; } = null!; //TODO nullable?
    public string LogDirectory { get; init; } = null!; //TODO nullable?
    public int MaxFileSize { get; init; } = DefaultMaxFileSize;
    public int WriteBatchTime { get; init; } = DefaultWriteBatchTime;
    public int ReadBatchTime { get; init; } = DefaultReadBatchTime;
}

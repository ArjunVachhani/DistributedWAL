namespace DistributedWAL;

//TODO use proper c# dotnet metrics
public class PoorTelemetry
{
    public static int WaitingForReader = 0;
    public static int ReaderWaitingForMoreData = 0;
    public static int FileWriteCount = 0;
    public static int FileReadCount = 0;
    public static long BytesWritten = 0;
    public static long BytesRead = 0;
    public static long BytesCopied = 0;
    public static long TimesCopied = 0;
    public static long BytesCopiedFast = 0;
    public static long TimesCopiedFast = 0;
}

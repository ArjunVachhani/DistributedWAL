using DistributedWAL;

namespace ConsoleApp;

internal class SampleStateMachine : IStateMachine
{
    public object ApplyLog(LogReader logReader)
    {
        return logReader.ReadInt32();
    }

    public object? ExecuteReadOperation(object? command)
    {
        return null;
    }

    public void RestoreSnapshot(Stream stream)
    {
    }

    public void SaveSnapshot(Stream stream)
    {
    }
}

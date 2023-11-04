using DistributedWAL;

namespace ConsoleApp;

internal class SampleStateMachine : IStateMachine
{
    public object ApplyLog(LogReader logReader)
    {
        var v = logReader.ReadInt32();
        logReader.GetSpan(256 - 8);
        var v2 = logReader.ReadInt32();
        if (v != v2)
        {

        }
        return v;
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

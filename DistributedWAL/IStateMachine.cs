namespace DistributedWAL;

public interface IStateMachine
{
    public object? ApplyLog(LogReader logReader);
    public object? ExecuteReadOperation(object? command);
    public void SaveSnapshot(Stream stream);
    public void RestoreSnapshot(Stream stream);
}

namespace DistributedWAL.Networking;

internal class Receiver : IDisposable
{
    private readonly Connection _connection;
    public Receiver(Connection connection)
    {
        _connection = connection;
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}

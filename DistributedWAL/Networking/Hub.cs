namespace DistributedWAL.Networking;

internal class Hub : IDisposable
{
    private readonly int _term;
    private readonly List<Sender> _senders;
    public Hub(int term)//TODO it should get list of nodes
    {
        _term = term;
        _senders = new List<Sender>();
    }

    public void Dispose()
    {
        foreach (Sender sender in _senders)
        {
            sender.Dispose();
        }
    }
}

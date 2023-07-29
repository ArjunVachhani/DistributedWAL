namespace DistributedWAL;

public class ConnectionSettings
{
    public ConnectionSettings(string clusterName, string clusterKey, IEnumerable<ConnectionConfiguration> initialConnection)
    {
        //TODO add more check for clusterName / clusterKey not empty and sufficient size, initial connection is more that 1
        ClusterName = clusterName;
        ClusterKey = clusterKey;
        InitialConnection = initialConnection;
    }

    public string ClusterName { get; init; }
    public string ClusterKey { get; init; }
    public IEnumerable<ConnectionConfiguration> InitialConnection { get; init; }
}

public class ConnectionConfiguration
{
    public ConnectionConfiguration(string host, int replicationPort, int discoveryPort)
    {
        //TODO add more checks for non empty host, port non 0 and port range
        Host = host;
        ReplicationPort = replicationPort;
        DiscoveryPort = discoveryPort;
    }

    public string Host { get; init; }
    public int ReplicationPort { get; init; }
    public int DiscoveryPort { get; init; }
}

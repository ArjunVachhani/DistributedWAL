namespace DistributedWAL
{
    internal class DistributedWalException : Exception
    {
        public DistributedWalException() { }

        public DistributedWalException(string message) : base(message) { }
    }
}

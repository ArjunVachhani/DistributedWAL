namespace DistributedWAL.Storage;

internal readonly struct FileEntry
{
    [Obsolete("Use parameterized constructor.")]
    public FileEntry() { }

    public FileEntry(int fileNumber, long firstLogIndex)
    {
        FileIndex = fileNumber;
        FirstLogIndex = firstLogIndex;
    }

    public readonly int FileIndex { get; init; }
    public readonly long FirstLogIndex { get; init; }
}

internal class FileEntryComparer : IComparer<FileEntry>
{
    int IComparer<FileEntry>.Compare(FileEntry x, FileEntry y)
    {
        return x.FirstLogIndex.CompareTo(y.FirstLogIndex);
    }

    internal static FileEntryComparer Default { get; private set; } = new FileEntryComparer();
}

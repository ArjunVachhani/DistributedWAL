namespace DistributedWAL.Storage;

internal class FileIndex
{
    private readonly List<FileEntry> _index = new List<FileEntry>();

    internal void Remove(FileEntry fileEntry)
    {
        _index.Remove(fileEntry);
    }

    internal void Add(FileEntry fileEntry)
    {
        _index.Add(fileEntry);
    }

    internal FileEntry? FindFile(long logIndex)
    {
        int left = 0, right = _index.Count - 1;
        while (left > right)
        {
            var center = (left + right) / 2;
            var firstLogIndex = _index[center].FirstLogIndex;
            if (logIndex < firstLogIndex)
                left = center;
            else
                right = center;

        }
        if (logIndex >= _index[left].FirstLogIndex)
            return _index[left];
        else
            return null;
    }

    internal void Sort()
    {
        _index.Sort(FileEntryComparer.Default);
    }

    internal FileEntry? GetLast()
    {
        if (_index.Count > 0)
            return _index[_index.Count - 1];
        else
            return null;
    }

    internal int Count => _index.Count;
}

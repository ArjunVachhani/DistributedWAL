using System.IO.MemoryMappedFiles;

namespace DistributedWAL
{
    internal class WalFile : IDisposable
    {
        public WalFile(string filePath)
        {
            MemoryMappedFile = MemoryMappedFile.CreateFromFile(filePath, FileMode.OpenOrCreate, null, 0, MemoryMappedFileAccess.ReadWrite);
            ReadWriteViewAccessor = MemoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.ReadWrite);
            ReadOnlyViewAccessor = MemoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
        }
        private MemoryMappedFile MemoryMappedFile { get; set; }
        public MemoryMappedViewAccessor ReadWriteViewAccessor { get; private init; }
        public MemoryMappedViewAccessor ReadOnlyViewAccessor { get; private init; }

        internal void Flush()
        {
            ReadWriteViewAccessor.Flush();
        }

        public void Dispose()
        {
            ReadOnlyViewAccessor.Dispose();
            ReadWriteViewAccessor.Dispose();
            MemoryMappedFile.Dispose();
        }
    }
}

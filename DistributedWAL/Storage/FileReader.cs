using System.IO.MemoryMappedFiles;

namespace DistributedWAL.Storage;


// this should not be thread safe class
internal class FileReader
{
	private readonly MemoryMappedViewAccessor _accessor;
	public FileReader(MemoryMappedViewAccessor accessor)
	{
		_accessor = accessor;
    }
}

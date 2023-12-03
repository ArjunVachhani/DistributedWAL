using DistributedWAL;
using System.Buffers.Binary;

namespace ConsoleApp;

internal class SampleStateMachine : IStateMachine
{
    public object ApplyLog(ReadOnlySpan<byte> bytes)
    {
        var v = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        var v2 = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(bytes.Length - 4));
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

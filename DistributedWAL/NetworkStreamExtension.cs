using System.Net.Sockets;
using System.Text;

namespace DistributedWAL;

internal static class NetworkStreamExtension
{
    public static void Write(this NetworkStream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[4];
        BitConverter.TryWriteBytes(bytes, value);
        stream.Write(bytes);
    }

    public static void WriteLengthPrefixed(this NetworkStream stream, string text)
    {
        var byteLength = Encoding.UTF8.GetByteCount(text);
        Write(stream, byteLength);
        Span<byte> bytes = byteLength <= 1024 ? stackalloc byte[byteLength] : new byte[byteLength];//TODO use arraypool
        stream.Write(bytes);
    }

    public static int ReadInt32(this NetworkStream stream)
    {
        Span<byte> bytes = stackalloc byte[4];
        stream.ReadExactly(bytes);
        return BitConverter.ToInt32(bytes);
    }

    public static string ReadUtf8(this NetworkStream stream, int byteLength)
    {
        Span<byte> data = byteLength <= 1024 ? stackalloc byte[byteLength] : new byte[byteLength];//TODO use arraypool
        stream.ReadExactly(data);
        return Encoding.UTF8.GetString(data);
    }

    public static string ReadUtf8LengthPrefixed(this NetworkStream stream)
    {
        var length = ReadInt32(stream);
        return ReadUtf8(stream, length);
    }
}

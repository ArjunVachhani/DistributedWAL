using System.Buffers.Binary;

namespace DistributedWAL.Storage;

internal static class FileLogHelper
{
    // returns true if content can be appended to file. false if file is sealed.
    internal static bool SeekToEndForWrite(FileStream fileStream)
    {
        if (fileStream.Position != 0)
            fileStream.Seek(0, SeekOrigin.Begin);

        Span<byte> bytes = stackalloc byte[Constants.MessagPayloadSize];
        var logSize = 0;
        do
        {
            fileStream.ReadExactly(bytes);
            logSize = BinaryPrimitives.ReadInt32LittleEndian(bytes);
            if (logSize > 0)
            {
                var seek = logSize + (Constants.MessageHeaderSize - Constants.MessagPayloadSize) + Constants.MessageTrailerSize;
                fileStream.Seek(seek, SeekOrigin.Current);
            }
            else if (logSize == -1)
                return false;
            else if (logSize == 0)
                fileStream.Seek(-Constants.MessagPayloadSize, SeekOrigin.Current);

        } while (logSize > 0 && fileStream.Length > fileStream.Position + 4);

        return true;
    }

    internal static void SeekToLog(FileStream fileStream, long logIndex)
    {
        if (fileStream.Position != 0)
            fileStream.Seek(0, SeekOrigin.Begin);

        Span<byte> bytes = stackalloc byte[8];
        var logSize = 0;
        do
        {
            fileStream.ReadExactly(bytes);
            logSize = BinaryPrimitives.ReadInt32LittleEndian(bytes);
            if (logSize > 0)
            {
                fileStream.ReadExactly(bytes);
                var currentLogIndex = BinaryPrimitives.ReadInt64LittleEndian(bytes);
                if (currentLogIndex == logIndex)
                {
                    fileStream.Seek(-Constants.MessageHeaderSize, SeekOrigin.Current);
                    break;
                }
                else
                {
                    var seek = logSize + (Constants.MessageHeaderSize - Constants.MessagPayloadSize) + Constants.MessageTrailerSize;
                    fileStream.Seek(seek, SeekOrigin.Current);
                }
            }
            else if (logSize == -1)
                break;
            else if (logSize == 0)
                fileStream.Seek(-bytes.Length, SeekOrigin.Current);

        } while (logSize > 0 && fileStream.Length > fileStream.Position + 4);
    }

    internal static void Seal(FileStream fileStream)
    {
        if (fileStream.Position + 4 >= fileStream.Length)
        {
            Span<byte> bytes = stackalloc byte[4];
            BitConverter.TryWriteBytes(bytes, -1);
            fileStream.Write(bytes);
        }
        fileStream.SetLength(fileStream.Position);
    }

    internal static void TruncateUnfinishedLog(FileStream fileStream)
    {
        if (fileStream.Position != 0)
            fileStream.Seek(0, SeekOrigin.Begin);

        Span<byte> bytes = stackalloc byte[Constants.MessagPayloadSize];
        var logSize = 0;
        do
        {
            fileStream.ReadExactly(bytes);
            logSize = BinaryPrimitives.ReadInt32LittleEndian(bytes);
            if (logSize > 0)
            {
                var seek = logSize + (Constants.MessageHeaderSize - Constants.MessagPayloadSize);
                fileStream.Seek(seek, SeekOrigin.Current);
                fileStream.ReadExactly(bytes);
                var logSizeEnd = BinaryPrimitives.ReadInt32LittleEndian(bytes);
                if (logSize != logSizeEnd)
                {
                    ZeroOutFile(fileStream, (int)fileStream.Position - (logSize + Constants.MessageOverhead));
                }
            }
            else if (logSize == -1)
                break;

        } while (logSize > 0 && fileStream.Length > fileStream.Position + 4);
    }

    private static void ZeroOutFile(FileStream fileStream, int startPosition)
    {
        fileStream.Seek(startPosition, SeekOrigin.Begin);
        var bytesToWrite = fileStream.Length - fileStream.Position;
        byte[] buffer = new byte[4096];
        while (bytesToWrite > 0)
        {
            var chunkSize = bytesToWrite >= buffer.Length ? buffer.Length : bytesToWrite;
            fileStream.Write(buffer, 0, (int)chunkSize);
        }
        fileStream.Seek(startPosition, SeekOrigin.Begin);
        fileStream.Flush();
    }
}

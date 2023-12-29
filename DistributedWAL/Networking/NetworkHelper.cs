using DistributedWAL.Storage;
using System.Buffers.Binary;
using System.Text;

namespace DistributedWAL.Networking;

internal static class NetworkHelper
{
    public static int GetNetworkMessageType(Stream stream, Span<byte> bytes)
    {
        stream.ReadExactly(bytes.Slice(0, NetworkConstants.NetworkMessageTypeSize));
        return BinaryPrimitives.ReadInt32LittleEndian(bytes);
    }

    public static void SendRequestVote(Stream stream, Span<byte> bytes, int term, string localHost, LogNumber logNumber)
    {
        BinaryPrimitives.WriteInt32LittleEndian(bytes, NetworkMessageType.RequestVote);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(4), term);
        var bytesCount = Encoding.UTF8.GetByteCount(localHost);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(8), bytesCount);
        Encoding.UTF8.GetBytes(localHost, bytes.Slice(12));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(12 + bytesCount), logNumber.Term);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.Slice(16 + bytesCount), logNumber.LogIndex);
        stream.Write(bytes.Slice(0, 24 + bytesCount));
    }

    public static (int RemoteTerm, string RemoteHost, LogNumber LogNumber) GetRequestVote(Stream stream, Span<byte> bytes)
    {
        stream.ReadExactly(bytes.Slice(0, 8));
        var remoteHostTerm = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        var hostNameLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(4));
        stream.ReadExactly(bytes.Slice(0, hostNameLength));
        var remoteHost = Encoding.UTF8.GetString(bytes.Slice(0, hostNameLength));
        stream.ReadExactly(bytes.Slice(0, 12));
        var logTerm = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        var logIndex = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(4));
        return (remoteHostTerm, remoteHost, new LogNumber(logTerm, logIndex));
    }

    public static void SendRequestVoteResponse(Stream stream, Span<byte> bytes, bool voteGranted, int term)
    {
        BinaryPrimitives.WriteInt32LittleEndian(bytes, NetworkMessageType.RequestVoteResponse);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(4), term);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(8), voteGranted ? 1 : 0);
        stream.Write(bytes.Slice(0, 12));
    }

    public static (int Term, bool VoteGranted) GetRequestVoteResponse(Stream stream, Span<byte> bytes)
    {
        stream.ReadExactly(bytes.Slice(0, 8));
        var term = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        var vote = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(4)) == 1;
        return (term, vote);
    }

    public static void SendAppendEntries(Stream stream, Span<byte> bytes, int logCount, int senderTerm, string? senderHostName, ref LogNumber lastSentLogNumber, IConsensus consensus, IFileReader fileReader)
    {
        BinaryPrimitives.WriteInt32LittleEndian(bytes, NetworkMessageType.AppendEntries);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(4), senderTerm);
        var hostBytesLength = !string.IsNullOrWhiteSpace(senderHostName) ? Encoding.UTF8.GetByteCount(senderHostName) : 0;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(8), hostBytesLength);
        if (!string.IsNullOrWhiteSpace(senderHostName))
            Encoding.UTF8.GetBytes(senderHostName, bytes.Slice(12));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(12 + hostBytesLength), logCount);
        if (logCount > 0)//TODO check if data is available in buffer
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(16 + hostBytesLength), lastSentLogNumber.Term);
            BinaryPrimitives.WriteInt64LittleEndian(bytes.Slice(20 + hostBytesLength), lastSentLogNumber.LogIndex);
            stream.Write(bytes.Slice(0, 28 + hostBytesLength));
            for (int i = 0; i < logCount; i++)
            {
                var log = fileReader.ReadNextLog();
                stream.Write(log.Slice(0, 4));//add size of log
                lastSentLogNumber = Utils.VerifyAndReadLogNumber(log);
                if (consensus.Term == senderTerm)
                    throw new DistributedWalException("Sender is not a leader.");
                stream.Write(log);//write full log. log size is duplicated as it is written in line 178
                fileReader.CompleteRead();
            }

            BinaryPrimitives.WriteInt64LittleEndian(bytes, consensus.CommittedLogIndex);
            stream.Write(bytes.Slice(0, 8));
        }
        else
        {
            BinaryPrimitives.WriteInt64LittleEndian(bytes.Slice(16 + hostBytesLength), consensus.CommittedLogIndex);
            stream.Write(bytes.Slice(0, 24 + hostBytesLength));
        }
    }

    public static (int ResultCode, long LogIndex) GetAppendEntries(Stream stream, Span<byte> bytes, IConsensus consensus)
    {
        stream.ReadExactly(bytes.Slice(0, 8));
        var senderTerm = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        var hostNameLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(4));
        string? hostName = null;
        if (hostNameLength > 0)
        {
            stream.ReadExactly(bytes.Slice(0, hostNameLength));
            hostName = Encoding.UTF8.GetString(bytes.Slice(0, hostNameLength));
        }
        stream.ReadExactly(bytes.Slice(0, 4));
        var logCount = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        if (logCount > 0)
        {
            stream.ReadExactly(bytes.Slice(0, 12));
            var prevLogTerm = BinaryPrimitives.ReadInt32LittleEndian(bytes);
            var prevLogIndex = BinaryPrimitives.ReadInt64LittleEndian(bytes);
            LogNumber prevLogNumber = new LogNumber(prevLogTerm, prevLogIndex);
            for (int i = 0; i < logCount; i++)
            {
                stream.ReadExactly(bytes.Slice(0, 4));
                var logSize = BinaryPrimitives.ReadInt32LittleEndian(bytes);
                var log = bytes.Slice(0, logSize + Constants.MessageOverhead);
                stream.ReadExactly(log);
                var logResult = consensus.AppendLog(hostName, senderTerm, prevLogNumber, log);
                if (logResult.ResultCode != AppendEntriesResultCode.Success)
                    return logResult;
            }
        }

        stream.ReadExactly(bytes.Slice(0, 8));
        var committedIndex = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        var result = consensus.AppendLog(hostName, senderTerm, committedIndex);
        return result;
    }

    public static void SendAppendEntriesResponse(Stream stream, Span<byte> bytes, int senderTerm, int resultCode, long savedLogIndex)
    {
        BinaryPrimitives.WriteInt32LittleEndian(bytes, NetworkMessageType.AppendEntriesResponse);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(4), senderTerm);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.Slice(8), resultCode);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.Slice(12), savedLogIndex);
        stream.Write(bytes.Slice(0, 20));
    }

    public static (int SenderTerm, int ResultCode, long LogIndex) GetAppendEntriesResponse(Stream stream, Span<byte> bytes)
    {
        stream.ReadExactly(bytes.Slice(0, 16));
        var senderTerm = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        var resultCode = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(4));
        var logIndex = BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8));
        return (senderTerm, resultCode, logIndex);
    }
}

/*
 * Networking message format
 * ---------------------------
 * First 4 bytes represent MessageType for request and response, 
 * Message does not have prefixed length because it would be hard to calculate for appendLogs message. 
 * as it can be variable and costly to know in the begning
 * 
 * Request Vote Format
 * [4 Byte Message Type] [4 byte Term] [4 Byte HostName length] [X bytes for Host Name] [4 byte last LogTerm] [8 byte last LogIndex]
 * 
 * Response : Request Vote Format
 * [4 Byte Message Type] [4 byte Term] [4 Byte to ACK/NACK]
 * 
 * AppendEntries Format
 * [4 Byte Message Type] [4 byte Sender Term] [4 Byte HostName Length] [X bytes for Host Name] [4 Bytes for message count] [4 byte Prev LogTerm] [8 Byte prev LogIndex] [Log M] [Log N] [Log O] [8 Committed LogIndex]
 * 
 * Response : AppendEntries Format
 * [4 Byte Message Type] [4 Byte Term] [4 byte ACK/NACK] [8 Byte last LogIndex]
 * 
 * InstallSnaphost Format
 * [4 Byte Message Type] [4 Byte Term] TBD
 * 
 * 
 * Response InstallSnapshot Format
 * [4 Byte Message type] [4 Byte Term] [4 Byte to ACK/NACK]
 * 
 */
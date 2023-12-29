using DistributedWAL.Networking;
using DistributedWAL.Storage;
using Moq;
using System.Buffers;

namespace DistributedWAL.Tests;

public class NetworkHelperTests
{
    public static IEnumerable<object[]> RequestVoteData =>
        new List<object[]>
        {
            new object[] { 0, "a", default(LogNumber) },
            new object[] { 1, "b", new LogNumber(1,1) },
            new object[] { 9879, "asdfml", new LogNumber(1812,9801478) },
            new object[] { int.MinValue, "foo bar", new LogNumber(int.MinValue,long.MinValue) },
            new object[] { int.MaxValue, "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse accumsan hendrerit ultricies. Fusce eleifend, nisi vitae ullamcorper porta, metus quam vestibulum leo, viverra mattis mi felis a enim. Mauris diam enim, iaculis nec laoreet eu, tincidunt at quam. Morbi gravida libero et turpis sollicitudin feugiat. Aenean vel ligula gravida, faucibus dui ut, dapibus tortor. In ut nibh hendrerit, porttitor lorem non, gravida elit. Maecenas sit amet est ac lectus luctus ornare ac nec turpis. Vivamus magna leo, finibus ut nibh quis, dignissim vestibulum lectus.", new LogNumber(int.MaxValue, long.MaxValue) },
        };

    [Theory]
    [MemberData(nameof(RequestVoteData))]
    public void RequestVote(int term, string hostName, LogNumber logNumber)
    {
        var stream = new MemoryStream();
        var bytes = ArrayPool<byte>.Shared.Rent(65536);
        NetworkHelper.SendRequestVote(stream, bytes, term, hostName, logNumber);
        stream.Seek(0, SeekOrigin.Begin);
        var messageType = NetworkHelper.GetNetworkMessageType(stream, bytes);
        Assert.Equal(NetworkMessageType.RequestVote, messageType);
        var result = NetworkHelper.GetRequestVote(stream, bytes);
        Assert.Equal(term, result.RemoteTerm);
        Assert.Equal(hostName, result.RemoteHost);
        Assert.Equal(logNumber.Term, result.LogNumber.Term);
        Assert.Equal(logNumber.LogIndex, result.LogNumber.LogIndex);
        ArrayPool<byte>.Shared.Return(bytes);
    }

    public static IEnumerable<object[]> RequestVoteResponseData =>
        new List<object[]>
        {
            new object[] { true, 1 },
            new object[] { false, 1 },
            new object[] { true, int.MinValue },
            new object[] { false, int.MinValue },
            new object[] { true, int.MaxValue },
            new object[] { false, int.MaxValue },
        };

    [Theory]
    [MemberData(nameof(RequestVoteResponseData))]
    public void RequestVoteResponse(bool voteGranted, int term)
    {
        var stream = new MemoryStream();
        var bytes = ArrayPool<byte>.Shared.Rent(65536);
        NetworkHelper.SendRequestVoteResponse(stream, bytes, voteGranted, term);
        stream.Seek(0, SeekOrigin.Begin);
        var messageType = NetworkHelper.GetNetworkMessageType(stream, bytes);
        Assert.Equal(NetworkMessageType.RequestVoteResponse, messageType);
        var result = NetworkHelper.GetRequestVoteResponse(stream, bytes);
        Assert.Equal(voteGranted, result.VoteGranted);
        Assert.Equal(term, result.Term);
        ArrayPool<byte>.Shared.Return(bytes);
    }

    public static IEnumerable<object[]> AppendEntriesResponseData =>
        new List<object[]>
        {
            new object[] { 0, 0, 0 },
            new object[] { -1, -1, -1L },
            new object[] { 1, 1, 1 },
            new object[] { 123, 234, 456 },
            new object[] { int.MinValue, int.MinValue, long.MinValue },
            new object[] { int.MaxValue, int.MaxValue, long.MaxValue}
        };

    [Theory]
    [MemberData(nameof(AppendEntriesResponseData))]
    public void AppendEntriesResponse(int senderTerm, int resultCode, long savedLogIndex)
    {
        var stream = new MemoryStream();
        var bytes = ArrayPool<byte>.Shared.Rent(65536);
        NetworkHelper.SendAppendEntriesResponse(stream, bytes, senderTerm, resultCode, savedLogIndex);
        stream.Seek(0, SeekOrigin.Begin);
        var messageType = NetworkHelper.GetNetworkMessageType(stream, bytes);
        Assert.Equal(NetworkMessageType.AppendEntriesResponse, messageType);
        var result = NetworkHelper.GetAppendEntriesResponse(stream, bytes);
        Assert.Equal(senderTerm, result.SenderTerm);
        Assert.Equal(resultCode, result.ResultCode);
        Assert.Equal(savedLogIndex, result.LogIndex);
        ArrayPool<byte>.Shared.Return(bytes);
    }

    public static IEnumerable<object[]> AppendEntriesNoLogsData =>
        new List<object[]>
        {
            new object[] { 0, 0, null, new LogNumber(0, 0), AppendEntriesResultCode.Success, 0L },
            new object[] { 1, 1, "foo", new LogNumber(1, 1), AppendEntriesResultCode.MissingLog, 1L },
            new object[] { 123, 234, "foo bar", new LogNumber(1979, 1896), AppendEntriesResultCode.HigherTerm, 182931739L },
            new object[] { int.MinValue, long.MinValue, "lorem", new LogNumber(int.MinValue, long.MinValue), AppendEntriesResultCode.HigherTerm, long.MinValue },
            new object[] { int.MaxValue, long.MaxValue, "ipsum", new LogNumber(int.MaxValue, long.MaxValue), AppendEntriesResultCode.HigherTerm, long.MaxValue },
        };

    [Theory]
    [MemberData(nameof(AppendEntriesNoLogsData))]
    public void AppendEntriesNoLogs(int senderTerm, long committedLogIndex, string senderHostName, LogNumber lastLogSentNumber, int receiverResultCode, long receiverSavedIndex)
    {
        var mockSenderConsensus = new Mock<IConsensus>();
        mockSenderConsensus.SetupGet(x => x.CommittedLogIndex).Returns(committedLogIndex);
        var mockSenderFileReader = new Mock<IFileReader>();
        var mockReceiverConsensus = new Mock<IConsensus>();
        mockReceiverConsensus.Setup(x => x.AppendLog(senderHostName, senderTerm, committedLogIndex)).Returns((receiverResultCode, receiverSavedIndex));
        var stream = new MemoryStream();
        var bytes = ArrayPool<byte>.Shared.Rent(65536);
        NetworkHelper.SendAppendEntries(stream, bytes, 0, senderTerm, senderHostName, ref lastLogSentNumber, mockSenderConsensus.Object, mockSenderFileReader.Object);
        stream.Seek(0, SeekOrigin.Begin);
        var messageType = NetworkHelper.GetNetworkMessageType(stream, bytes);
        Assert.Equal(NetworkMessageType.AppendEntries, messageType);
        var result = NetworkHelper.GetAppendEntries(stream, bytes, mockReceiverConsensus.Object);
        Assert.Equal(receiverResultCode, result.ResultCode);
        Assert.Equal(receiverSavedIndex, result.LogIndex);
        mockSenderConsensus.VerifyAll();
        mockSenderFileReader.VerifyAll();
        mockReceiverConsensus.VerifyAll();
        mockSenderConsensus.VerifyNoOtherCalls();
        mockSenderFileReader.VerifyNoOtherCalls();
        mockReceiverConsensus.VerifyNoOtherCalls();
        ArrayPool<byte>.Shared.Return(bytes);
    }
}

using DistributedWAL;
using System.Buffers.Binary;
using System.Diagnostics;

namespace ConsoleApp;

internal class Program
{
    static void Main(string[] args)
    {
        var config = new DistributedWalConfig()
        {
            MaxFileSize = int.MaxValue,
            LogDirectory = "C:\\Users\\Arjun_Vachhani\\Desktop\\wallog"
        };
        DistributedWal<SampleStateMachine> distributedWal = DistributedWal.DangerousCreateNewDistributedWal<SampleStateMachine>(config);

        if (distributedWal.NodeRole == NodeRoles.Leader)
        {
            //add log
            //var pub = distributedWal.AddPublication();
            //pub.AppendLog(20);

            // perform read action
            //distributedWal.Read(new Span<byte>());
        }
        else
        {

        }

        distributedWal.RegisterLogResultCallback(StatusCallback);

        var sw = Stopwatch.StartNew();
        var mesageSize = 256;
        var arrayLen = mesageSize;
        byte[] bytes = new byte[arrayLen];
        for (int j = 0; j < int.MaxValue / (mesageSize + 20); j++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes, j);
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(bytes.Length - 4), j);
            distributedWal.WriteLog(bytes);
        }

        Console.WriteLine(sw.ElapsedMilliseconds);
        sw.Restart();

        Console.WriteLine("WaitingForReader " + PoorTelemetry.WaitingForReader);
        Console.WriteLine("ReaderWaitingForMoreData " + PoorTelemetry.ReaderWaitingForMoreData);
        Console.WriteLine("FileWriteCount " + PoorTelemetry.FileWriteCount);
        Console.WriteLine("BytesWritten " + PoorTelemetry.BytesWritten);

        distributedWal.Flush();



        Console.WriteLine("Flush " + sw.ElapsedMilliseconds);
        sw.Restart();


        //var sw = Stopwatch.StartNew();
        //var mesageSize = 256;

        for (int j = 0; j < int.MaxValue / (mesageSize + 20); j++)
        {
            distributedWal.ApplyCommittedLogs();
        }

        //distributedWal.StopAsync().GetAwaiter().GetResult();

        Console.WriteLine("Apply time" + sw.ElapsedMilliseconds);
        Console.WriteLine("FileReadCount " + PoorTelemetry.FileReadCount);
        Console.WriteLine("BytesRead " + PoorTelemetry.BytesRead);
        Console.WriteLine("BytesCopied " + PoorTelemetry.BytesCopied);
        Console.WriteLine("TimesCopied " + PoorTelemetry.TimesCopied);
        Console.WriteLine("BytesCopiedFast " + PoorTelemetry.BytesCopiedFast);
        Console.WriteLine("TimesCopiedFast " + PoorTelemetry.TimesCopiedFast);
        //distributedWal.Stop().GetAwaiter().GetResult()
    }



    static void StatusCallback(LogNumber logNumber, object? result)
    {
        if (result is int xyx)
        {

        }
    }
}
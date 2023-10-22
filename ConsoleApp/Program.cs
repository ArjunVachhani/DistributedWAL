using DistributedWAL;
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
            //var pub  = distributedWal.AddPublication();
            //pub.AppendLog(20);

            // perform read action
            //distributedWal.Read(new Span<byte>());
        }
        else
        {

        }

        distributedWal.RegisterLogResultCallback(StatusCallback);
        var publication = distributedWal.AddPublication();

        var sw = Stopwatch.StartNew();
        var mesageSize = 1024;
        var arrayLen = mesageSize;
        byte[] bytes = new byte[arrayLen];
        for (int j = 0; j < int.MaxValue / (mesageSize + 20); j++)
        {
            var logger = publication.AppendFixedLengthLog(mesageSize);
            logger.Write(j);
            logger.Write(bytes, 0, bytes.Length - 8);
            logger.Write(j);
            logger.FinishLog();
        }

        distributedWal.ExecuteReadOperation(null);

        Console.WriteLine(sw.ElapsedMilliseconds);
        sw.Restart();

        distributedWal.Flush();

        Console.WriteLine(sw.ElapsedMilliseconds);
        sw.Restart();


        for (int j = 0; j < int.MaxValue / (mesageSize + 20); j++)
        {
            distributedWal.ApplyCommittedLogs();
        }

        distributedWal.StopAsync().GetAwaiter().GetResult();

        Console.WriteLine(sw.ElapsedMilliseconds);
        //distributedWal.Stop().GetAwaiter().GetResult()
    }



    static void StatusCallback(LogNumber logNumber, object? result)
    {
        if (result is int xyx)
        {

        }
    }
}
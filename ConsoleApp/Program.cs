using DistributedWAL;
using System.Diagnostics;

namespace ConsoleApp
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var config = new DistributedWalConfig()
            {
                MaxFileSize = int.MaxValue,
                FilePath = "C:\\Users\\Arjun_Vachhani\\Desktop\\wallog"
            };
            DistributedWal distributedWal = DistributedWal.DangerousCreateNewDistributedWal(config);
            var publication = distributedWal.AddPublication(StatusCallback);
            var sw = Stopwatch.StartNew();
            byte[] bytes = new byte[128];
            for (int j = 0; j < 2041330; j++)
            {
                var logger = publication.AppendFixedLengthLog(1024);

                for (int i = 0; i < 1024; i += 128)
                {
                    logger.Write(bytes, 0, bytes.Length);
                }

                //logger.Write(bytes, 0, bytes.Length);

                logger.FinishLog();
            }
            Console.WriteLine(sw.ElapsedMilliseconds);
        }

        static void StatusCallback(long logIndex, int status)
        {
        }
    }
}

class StateMachine
{
    void InitProcess()
    {
        DistributedWal.ResumeDistibutedWal(null!);
        DistributedWal dw = DistributedWal.DangerousCreateNewDistributedWal(null!);
        long lastIndex = 0;//Get From somewhere
        var subscription = dw.AddSubscriber(ProcessLog, 123);
        while (!CancellationToken.None.IsCancellationRequested)
        {
            var t = subscription.ProcessNext();
            if (t != null)
                lastIndex = t.Value;
            else
                Thread.Yield();
        }
        lastIndex = 12;//save somewhere

        var publication = dw.AddPublication(StatusCallback);
        if (publication.NodeRole == NodeRoles.Leader)
        {
            var logWriter = publication.AppendFixedLengthLog(100);
            logWriter.Write(1);
            logWriter.FinishLog();
        }
    }

    void StatusCallback(long logIndex, int status)
    {
    }

    void ProcessLog(LogReader logReader)
    {
        logReader.ReadInt32();
    }
}

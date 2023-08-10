﻿namespace DistributedWAL.Tests
{
    public class WalFileManagerTests
    {
        [Theory]
        [InlineData(16 * 1024 * 1024, 100)]
        [InlineData(16 * 1024 * 1024, 500)]
        [InlineData(32 * 1024 * 1024, 100)]
        [InlineData(32 * 1024 * 1024, 500)]
        public void RequestWriteSegmentSingleWrite(int fileSize, int segmentSize)
        {
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var walFileManager = new WalFileManager(fileSize, path);
            (var lastLogIndex, var lastLogTerm) = walFileManager.Initialize();
            (var viewAccessor, var position) = walFileManager.RequestWriteSegment(segmentSize, true);
            Assert.Single(Directory.GetFiles(path));
            Assert.Equal(fileSize, new FileInfo(Directory.GetFiles(path).First()).Length);
            Assert.NotNull(viewAccessor);
            Assert.Equal(0, position);
            Assert.Equal(-1, lastLogIndex);
            Assert.Equal(-1, lastLogTerm);
            walFileManager.Dispose();
            Directory.Delete(path, true);
        }

        [Theory]
        [InlineData(16 * 1024 * 1024, 100, 3)]
        [InlineData(16 * 1024 * 1024, 500, 8)]
        [InlineData(32 * 1024 * 1024, 100, 4)]
        [InlineData(32 * 1024 * 1024, 500, 12)]
        public void RequestWriteSegmentMultipleRequestWrite(int fileSize, int segmentSize, int requestWriteCount)
        {
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var walFileManager = new WalFileManager(fileSize, path);
            (var lastLogIndex, var lastLogTerm) = walFileManager.Initialize();
            for (int i = 0; i < requestWriteCount; i++)
            {
                (var viewAccessor, var position) = walFileManager.RequestWriteSegment(segmentSize, true);
                Assert.Single(Directory.GetFiles(path));
                Assert.Equal(fileSize, new FileInfo(Directory.GetFiles(path).First()).Length);
                Assert.NotNull(viewAccessor);
                Assert.Equal(segmentSize * i, position);
            }
            walFileManager.Dispose();
            Directory.Delete(path, true);
        }
    }
}

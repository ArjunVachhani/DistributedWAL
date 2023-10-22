namespace DistributedWAL;

internal class Constants
{
    //Message layout
    //[4 payload size] + [4 term] + [8 logIndex]  + [X message] +  [4 length]
    //So 1 byte message will take 21 bytes to save, 10 bytes message will take 30 bytes to save. and so on.

    public const int MessageHeaderSize = 16; //[4 payload size] + [4 term] + [8 logIndex] 
    public const int MessageTrailerSize = 4; //[4 payload size]
    public const int MessageOverhead = MessageHeaderSize + MessageTrailerSize;


    //TODO add flags to store to message
}

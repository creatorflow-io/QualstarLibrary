namespace QualstarLibrary
{
    public class DriveEventArgs
    {
        public uint SlotNumber { get; init; }
        public string Operation { get; init; }
        public DriveEventArgs(uint slotNumber, string operation)
        {
            SlotNumber = slotNumber;
            Operation = operation;
        }
    }
}

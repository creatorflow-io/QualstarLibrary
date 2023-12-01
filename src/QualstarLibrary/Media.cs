namespace QualstarLibrary
{
    public class Media
    {
        public uint? StorageSlot { get; private set; }
        public uint? DriveSlot { get; private set; }
        public string VolumeTag { get; init; }
        public long? Capacity { get; private set; }
        public long? Remaining { get; private set; }
        public bool IsCleaner { get; private set; }
        public bool IsWriteProtected
        {
            get; private set;
        }

        public bool IsInDrive => DriveSlot.HasValue;
        public bool IsInStorage => StorageSlot.HasValue && !DriveSlot.HasValue;
        public bool IsExternal => !StorageSlot.HasValue && !DriveSlot.HasValue;

        public Media(string volumeTag)
        {
            VolumeTag = volumeTag;
            IsCleaner = volumeTag.StartsWith("CLN", StringComparison.OrdinalIgnoreCase)
                || volumeTag.EndsWith("CL", StringComparison.OrdinalIgnoreCase);
        }
        public Media(string volumeTag, uint storageSlot) : this(volumeTag)
        {
            StorageSlot = storageSlot;
        }

        public void SetSizeInfo(long? size, long? available)
        {
            Capacity = size;
            Remaining = available;
        }

        public void LoadedToDrive(uint driveSlotNumber)
        {
            DriveSlot = driveSlotNumber;
        }

        public void UnloadedFromDrive()
        {
            DriveSlot = default;
        }

        public void TransferedTo(uint slotNumber)
        {
            DriveSlot = default;
            StorageSlot = slotNumber;
        }

        public void StoredExternally()
        {
            DriveSlot = default;
            StorageSlot = default;
        }

        public void WriteProtected()
        {
            IsWriteProtected = true;
        }
    }
}

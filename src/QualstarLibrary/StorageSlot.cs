namespace QualstarLibrary
{
    public class StorageSlot
    {
        public uint SlotNumber { get; init; }
        public Media? Media { get; private set; }
        public bool IsFull => Media != null;
        /// <summary>
        /// True if I/O Port Slot, False if Cartridge Slot
        /// </summary>
        public bool IsIO { get; private set; }
        public StorageSlot(uint slotNumber, bool isIO)
        {
            SlotNumber = slotNumber;
            IsIO = isIO;
        }

        public void StoredMedia(Media media)
        {
            Media = media;
            Media.TransferedTo(SlotNumber);
        }

        public bool IsLoadedMedia(string volumeTag)
        {
            return volumeTag.Equals(Media?.VolumeTag, StringComparison.OrdinalIgnoreCase);
        }
        public void Empty()
        {
            Media = null;
        }
    }
}

namespace QualstarLibrary
{
    public class Drive
    {
        /// <summary>
        /// The drive's slot number.
        /// </summary>
        public uint SlotNumber { get; init; }
        /// <summary>
        /// Required for windows
        /// </summary>
        public string Address { get; init; }

        public string? Serial { get; private set; }
        /// <summary>
        /// Address for windows and device name for linux
        /// </summary>
        public string DeviceName { get; private set; }
        /// <summary>
        /// Mounted name for linux or drive letter for windows
        /// </summary>
        public string? MountPoint { get; private set; }
        public LtfsStatus Status { get; private set; }

        public Media? LoadedMedia { get; private set; }
        public bool IsFull => LoadedMedia != null;
        public bool IsWriteProtected => LoadedMedia?.IsWriteProtected ?? false;
        public bool IsAssigned => !string.IsNullOrEmpty(MountPoint);
        /// <summary>
        /// It is safe to unload
        /// </summary>
        public bool IsReleased { get; private set; }
        public int FailedCount => _failedTapes.Count;
        private HashSet<string> _failedTapes = new HashSet<string>();

        /// <summary>
        /// Current loaded cartridge slot number.
        /// </summary>
        public uint? LoadedSlotNumber { get; private set; }

        public Drive(uint slotNumber, string address, string? serial)
        {
            SlotNumber = slotNumber;
            Address = address;
            // default for windows
            DeviceName = address;
            Serial = serial;
        }

        public void AssignedTo(string mountPoint)
        {
            MountPoint = mountPoint;
            IsReleased = false;
        }

        public void Unassigned()
        {
            MountPoint = null;
        }

        /// <summary>
        /// Only for linux
        /// </summary>
        /// <param name="deviceName"></param>
        public void SetDeviceName(string deviceName)
        {
            DeviceName = deviceName;
        }

        public void WriteProtected()
        {
            SetStatus(LtfsStatus.LTFS_READ_ONLY);
        }

        public void SetSerial(string serial)
        {
            Serial = serial;
        }

        public void SetStatus(LtfsStatus status)
        {
            Status = status;
            if (status != LtfsStatus.NO_MEDIA && status != LtfsStatus.RESET)
            {
                IsReleased = false;
            }
            if (status == LtfsStatus.LTFS_READ_ONLY)
            {
                LoadedMedia?.WriteProtected();
            }
        }

        public void LoadMedia(Media media)
        {
            LoadedMedia = media;
            LoadedMedia.LoadedToDrive(SlotNumber);
            LoadedSlotNumber = media.StorageSlot;
        }

        public void UnloadMedia()
        {
            LoadedMedia = null;
            LoadedSlotNumber = null;
        }

        public void Release()
        {
            IsReleased = true;
            Status = LtfsStatus.NO_MEDIA;
        }

        public void Failed()
        {
            if (LoadedMedia != null)
            {
                _failedTapes.Add(LoadedMedia.VolumeTag);
            }
        }

        public void SetDriveInfo(DriveInfo driveInfo)
        {
            if (driveInfo.IsReady && LoadedMedia != null)
            {
                LoadedMedia.SetSizeInfo(driveInfo.TotalSize, driveInfo.AvailableFreeSpace);
            }
        }

        public void SetSizeInfo(long totalSize, long availableFreeSpace)
        {
            if (LoadedMedia != null)
            {
                LoadedMedia.SetSizeInfo(totalSize, availableFreeSpace);
            }
        }

        public bool IsLoadedMedia(string volumeTag)
        {
            return LoadedMedia != null && LoadedMedia.VolumeTag.Equals(volumeTag, StringComparison.OrdinalIgnoreCase);
        }

    }
}

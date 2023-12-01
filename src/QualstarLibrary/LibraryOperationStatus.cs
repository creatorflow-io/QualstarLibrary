using System.ComponentModel.DataAnnotations;

namespace QualstarLibrary
{
    public enum LibraryOperationStatus
    {
        NoAction,
        Succeeded,
        Failed,
        /// <summary>
        /// operation maybe take a long time and continue after this result
        /// <para>pleas check the status of library manually</para>
        /// </summary>
        Ongoing,
        DriveNotFound,
        [Display(Description = "The requested tape was not found in any of the storage elements")]
        TapeNotFound,
        NotSupported,
        MtxBusy,
        DriveBusy,
        /// <summary>
        /// LTFS is being managed by another process
        /// </summary>
        [Display(Description = "LTFS is being managed by another process")]
        LTFS60086E = 60086,
        /// <summary>
        /// Invalid ltfs status
        /// <para>While performing an operation, such as check, eject, format, or rollback, the state of the medium could be changed by another session.</para>
        /// <para>Before attempting to perform the operation, check the icon displayed in Wnidows Explorer.</para>
        /// </summary>
        [Display(Description = "Invalid ltfs status")]
        LTFS60233E = 60233,
        /// <summary>
        /// Error mounting tape. The tape drive is busy or not functioning properly.
        /// <para>Check the drive. If the problem persists, reboot the system.</para>
        /// </summary>
        [Display(Description = "Error mounting tape. The tape drive is busy or not functioning properly")]
        LTFS60201E = 60201,
        /// <summary>
        /// The medium cannot be mounted because the cartridge is not partitioned for LTFS.
        /// <para>Format the cartridge for LTFS in order to mount it. Note that formatting a cartridge erases the data on that cartridge</para>
        /// </summary>
        [Display(Description = "The medium cannot be mounted because the cartridge is not partitioned for LTFS")]
        LTFS17168E = 17168,
        /// <summary>
        /// Cannot load the medium: failed to determine medium position
        /// </summary>
        [Display(Description = "Cannot load the medium: failed to determine medium position")]
        LTFS12019E = 12019,
        /// <summary>
        /// Failed to load the cartridge (ltfs_load_tape)
        /// <para>Damaged tape or drive</para>
        /// </summary>
        [Display(Description = "Failed to load the cartridge (ltfs_load_tape)")]
        LTFS11331E = 11331,
        /// <summary>
        /// Cannot read volume: failed to load the tape
        /// </summary>
        [Display(Description = "Cannot read volume: failed to load the tape")]
        LTFS11006E = 11006,
        /// <summary>
        /// Cannot format: medium is write-protected
        /// </summary>
        [Display(Description = "Cannot format: medium is write-protected")]
        LTFS11095E = 11095,
        /// <summary>
        /// Medium formatted successfully.
        /// </summary>
        [Display(Description = "Medium formatted successfully")]
        LTFS15024I = 15024,
        /// <summary>
        /// Medium is already formatted (0).
        /// </summary>
        [Display(Description = "Medium is already formatted (0)")]
        LTFS15047E = 15047,
        /// <summary>
        /// Volume is consistent.
        /// </summary>
        [Display(Description = "Volume is consistent")]
        LTFS16022I = 16022,
        /// <summary>
        ///  Volume is inconsistent and was not corrected
        /// </summary>
        [Display(Description = "Volume is inconsistent and was not corrected")]
        LTFS16021E = 16021,
        /// <summary>
        /// Volume is inconsistent. Try to recover consistency with ltfsck first
        /// </summary>
        [Display(Description = "Volume is inconsistent. Try to recover consistency with ltfsck first")]
        LTFS16087E = 16087,
        /// <summary>
        /// Volume unmounted successfully.
        /// </summary>
        [Display(Description = "Volume unmounted successfully")]
        LTFS11034I = 11034,
        /// <summary>
        /// Volume mounted successfully.
        /// </summary>
        [Display(Description = "Volume mounted successfully")]
        LTFS11031I = 11031,
        /// <summary>
        /// Cannot get mount point (0)
        /// </summary>
        [Display(Description = "Cannot get mount point (0)")]
        LTFS14094E = 14094,
        /// <summary>
        /// Cannot rewind medium: backend call failed
        /// <para>ltfsck to detemine the tape damaged</para>
        /// </summary>
        [Display(Description = "Cannot rewind medium: backend call failed")]
        LTFS12035E = 12035,
        /// <summary>
        /// No medium in drive
        /// </summary>
        [Display(Description = "No medium in drive")]
        LTFS12016E = 12016,
        /// <summary>
        /// Cannot open device
        /// <para>Verify that the drive is powered on and connected to the system. The tape drive must be powered on prior to powering on the system or the tape drive cannot be recognized. If the tape drive is used by another program, unmount or release the drive from the program.</para>
        /// </summary>
        [Display(Description = "Cannot open device")]
        LTFS10004E = 10004,
        /// <summary>
        /// Cannot open device
        /// <para>Verify that the drive is powered on and connected to the system. The tape drive must be powered on prior to powering on the system or the tape drive cannot be recognized. If the tape drive is used by another program, unmount or release the drive from the program.</para>
        /// </summary>
        [Display(Description = "Cannot open device")]
        LTFS12012E = 12012,
    }

    public static class LibraryOperationStatusExtensions
    {
        public static bool IsFinallyError(this LibraryOperationStatus status)
        {
            return status switch
            {
                LibraryOperationStatus.LTFS60233E => true,
                LibraryOperationStatus.LTFS60201E => true,
                LibraryOperationStatus.LTFS17168E => true,
                LibraryOperationStatus.LTFS11331E => true,
                LibraryOperationStatus.LTFS11095E => true,
                LibraryOperationStatus.LTFS16021E => true,
                LibraryOperationStatus.LTFS16087E => true,
                LibraryOperationStatus.LTFS12035E => true,
                LibraryOperationStatus.LTFS12016E => true,
                LibraryOperationStatus.LTFS12012E => true,
                LibraryOperationStatus.LTFS10004E => true,
                LibraryOperationStatus.LTFS11006E => true,
                LibraryOperationStatus.LTFS12019E => true,
                _ => false,
            };
        }

        public static bool IsEjectable(this LibraryOperationStatus status)
        {
            return status switch
            {
                LibraryOperationStatus.LTFS11331E => true,
                LibraryOperationStatus.LTFS12035E => true,
                LibraryOperationStatus.LTFS12016E => true,
                LibraryOperationStatus.LTFS11006E => true,
                LibraryOperationStatus.LTFS12019E => true,
                _ => false,
            };
        }

        public static bool IsSuccess(this LibraryOperationStatus status)
        {
            return status switch
            {
                LibraryOperationStatus.Succeeded => true,
                LibraryOperationStatus.NoAction => true,
                LibraryOperationStatus.LTFS15024I => true,
                LibraryOperationStatus.LTFS16022I => true,
                LibraryOperationStatus.LTFS11034I => true,
                LibraryOperationStatus.LTFS11031I => true,
                _ => false,
            };
        }

    }
}

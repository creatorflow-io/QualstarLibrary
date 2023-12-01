using System.Collections.Concurrent;

namespace QualstarLibrary
{
    public interface ILibrary
    {
        Drive[] Drives { get; }
        StorageSlot[] Slots { get; }

        event EventHandler<DriveEventArgs>? DriveChanged;
        event EventHandler<MediaEventArgs>? MediaChanged;
        event EventHandler<OperationLoggingEventArgs>? OperationLogging;

        ConcurrentDictionary<string, LibraryOperation> Operations { get; }

        /// <summary>
        /// Collect Drive and Slot status
        /// </summary>
        /// <param name="force"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task CollectStatusAsync(bool force, CancellationToken token);
        /// <summary>
        /// Check if library is ready to use
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<bool> IsReadyAsync(CancellationToken token);

        /// <summary>
        /// Load cartridge to drive and try to mount it
        /// </summary>
        /// <param name="volumeTag"></param>
        /// <param name="driveSlotNumber"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> LoadAsync(string volumeTag, uint driveSlotNumber, CancellationToken token);
        /// <summary>
        /// Unmount cartridge and unload it from drive
        /// </summary>
        /// <param name="driveSlotNumber"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> UnloadAsync(uint driveSlotNumber, CancellationToken token);

        /// <summary>
        /// Mount ltfs volume on drive
        /// </summary>
        /// <param name="driveSlotNumber"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> MountAsync(uint driveSlotNumber, CancellationToken token);
        /// <summary>
        /// Unmount ltfs volume on drive
        /// </summary>
        /// <param name="driveSlotNumber"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> UnmountAsync(uint driveSlotNumber, CancellationToken token);

        /// <summary>
        /// Unload cartridge from its slot to other storage slot
        /// </summary>
        /// <param name="volumeTag"></param>
        /// <param name="targetStorageSlotNumber"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> TransferAsync(string volumeTag, uint targetStorageSlotNumber, CancellationToken token);
        /// <summary>
        /// Execute ltfsck command on drive
        /// </summary>
        /// <param name="driveSlotNumber"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> LtfsckAsync(uint driveSlotNumber, CancellationToken token);
        /// <summary>
        /// Execute ltfs format command on drive
        /// </summary>
        /// <param name="driveSlotNumber"></param>
        /// <param name="force"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> FormatAsync(uint driveSlotNumber, bool force, CancellationToken token);

        /// <summary>
        /// Release the resources to shutdown library
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<LibraryOperation> ReleaseAsync(CancellationToken token);
    }

    public static class LibraryExtensions
    {
        public static Task<Media[]> GetMediasAsync(this ILibrary library, CancellationToken token)
        {
            var medias = new List<Media>();
            foreach (var drive in library.Drives)
            {
                if (drive.IsFull)
                {
                    medias.Add(drive.LoadedMedia!);
                }
            }
            foreach (var slot in library.Slots)
            {
                if (slot.IsFull)
                {
                    medias.Add(slot.Media!);
                }
            }
            return Task.FromResult(medias.ToArray());
        }

        #region Operations maintainance
        public static LibraryOperation? GetOperation(this ILibrary library, string traceId)
        {
            library.ClearOperations();
            return library.Operations.ContainsKey(traceId)
            ? library.Operations[traceId]
            : default;
        }

        static TimeSpan _cleanupTime = TimeSpan.FromMinutes(60).Negate();
        public static void ClearOperations(this ILibrary library)
        {
            var values = library.Operations.Values.ToArray();
            foreach (var op in values)
            {
                if (op.Status != LibraryOperationStatus.Ongoing
                    && op.Timestamp < DateTimeOffset.Now.Add(_cleanupTime))
                {
                    library.Operations.TryRemove(op.TraceId, out var _);
                }
            }
        }
        #endregion
    }
}

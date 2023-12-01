using Juice.Locks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.RegularExpressions;

namespace QualstarLibrary.Services.Windows
{
    internal class Library : LibraryBase
    {
        protected override Services.LibraryOptions Options => _options.CurrentValue;
        private IOptionsMonitor<LibraryOptions> _options;
        public Library(ILoggerFactory logger, IDistributedLock locker,
            IOptionsMonitor<LibraryOptions> options,
            IOperationRepository? operationRepository = null) : base(logger.CreateLogger("QualstarLibrary.Windows"), locker, operationRepository)
        {
            _options = options;
            if (!options.CurrentValue.MtxChanger.HasValue)
            {
                throw new ArgumentException("MtxChanger is not set");
            }
            MtxChanger = $"Changer{options.CurrentValue.MtxChanger}";
        }

        public override async Task CollectStatusAsync(bool force, CancellationToken token)
        {
            await base.CollectStatusAsync(force, token);
            await UpdateLtfsStatusAsync(default, token);
        }

        #region Ltfs
        string drivesPattern = @"^(?<assigned>[\w]{0,1})[\s]+(?<address>[0-9\.]+)[\s]+(?<serial>[\S]+)[\s]+(?<status>[A-Z_]+)";

        private async Task<LibraryOperation> AssignAsync(Drive drive, CancellationToken token)
        {
            if (!drive.IsAssigned)
            {
                var label = GetAssignableLabelFromZToE();
                if (label == null)
                {
                    return new(LibraryOperationStatus.Failed, $"No more assignable label");
                }
                var rs = await LtfsAssignAsync(default, drive.Address, label);
                if (rs.Status.IsSuccess())
                {
                    drive.AssignedTo(label);
                    await Task.Delay(1000, token);
                }

                return new(rs.Status, rs.Message);
            }
            else
            {
                return new(LibraryOperationStatus.Succeeded, $"Drive {drive.Address} is already assigned to {drive.MountPoint}");
            }
        }

        private async Task UpdateLtfsStatusAsync(string? traceId, CancellationToken token)
        {
            var ltfsDrives = Options.LtfsPath == null ? "LtfsCmdDrives"
                : Path.Combine(Options.LtfsPath, "LtfsCmdDrives");

            await ExecAsync(ltfsDrives, "", traceId, token, (line) =>
            {
                var match = Regex.Match(line, drivesPattern);
                if (match.Success)
                {
                    var assigned = match.Groups["assigned"].Value;
                    var address = match.Groups["address"].Value;
                    var serial = match.Groups["serial"].Value;
                    var status = match.Groups["status"].Value;
                    var drive = Drives.FirstOrDefault(d => d.Address == address);
                    if (drive != null)
                    {
                        if (!string.IsNullOrEmpty(assigned))
                        {
                            drive.AssignedTo(assigned);
                        }
                        else
                        {
                            drive.Unassigned();
                        }
                        drive.SetSerial(serial);
                        if (Enum.TryParse(status, out LtfsStatus ltfsStatus))
                        {
                            drive.SetStatus(ltfsStatus);
                        }
                    }
                    else
                    {
                        _logger.LogWarning($"Drive {address} is not configured");
                    }
                }
            });
        }

        private async Task<(LibraryOperationStatus Status, string? Message)> LtfsAssignAsync(string? traceId, string address, string label)
        {
            var ltfsAssign = Options.LtfsPath == null ? "LtfsCmdAssign" : Path.Combine(Options.LtfsPath, "LtfsCmdAssign");
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var rs = await ExecAsync(ltfsAssign, $"{label} {address}", traceId, default, message =>
            {
                if (TryFindLtfsKnownMessage(message, out var s))
                {
                    foundMessage = message;
                    foundStatus = s;
                }
            }).ConfigureAwait(false);

            var rt = (foundStatus ??
                (rs.ExitCode == 0 ? LibraryOperationStatus.Succeeded
                : LibraryOperationStatus.Failed), foundMessage ?? rs.Output);

            Log(traceId, $"Assign {address} to {label} status: {rt.Item1} {rt.Item2 ?? ""}");
            return rt;
        }

        private async Task<(LibraryOperationStatus Status, string? Message)> LtfsUnassignAsync(string? traceId, string label)
        {
            var ltfsUnassign = Options.LtfsPath == null ? "LtfsCmdUnassign" : Path.Combine(Options.LtfsPath, "LtfsCmdUnassign");
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var rs = await ExecAsync(ltfsUnassign, label, traceId, default, message =>
            {
                if (TryFindLtfsKnownMessage(message, out var s))
                {
                    foundMessage = message;
                    foundStatus = s;
                }
            }).ConfigureAwait(false);
            var rt = (foundStatus ??
                (rs.ExitCode == 0 ? LibraryOperationStatus.Succeeded
                : LibraryOperationStatus.Failed), foundMessage ?? rs.Output);

            Log(traceId, $"Unassign {label} status: {rt.Item1} {rt.Item2 ?? ""}");

            return rt;
        }

        private string? GetAssignableLabelFromZToE(string? from = default)
        {
            var drives = DriveInfo.GetDrives();
            var label = from != null ? ((char)(from[0] - 1)).ToString() : "Z";
            while (true)
            {
                if (!drives.Any(d => d.Name.StartsWith(label)))
                {
                    return label;
                }
                label = ((char)(label[0] - 1)).ToString();
                if (label == "D")
                {
                    return null;
                }
            }
        }

        protected override async Task<(LibraryOperationStatus Status, string? Message)> LtfsMountAsync(Drive drive, string traceId, CancellationToken token)
        {
            Log(traceId, $"Mounting drive {drive.DeviceName}...");

            var ltfsLoad = Options.LtfsPath == null ? "LtfsCmdLoad" : Path.Combine(Options.LtfsPath, "LtfsCmdLoad");
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var rs = await ExecAsync(ltfsLoad, drive.MountPoint!, traceId, token, message =>
            {
                if (TryFindLtfsKnownMessage(message, out var s))
                {
                    foundMessage = message;
                    foundStatus = s;
                }
            });
            if (foundStatus == LibraryOperationStatus.LTFS60233E)
            {
                await UpdateLtfsStatusAsync(traceId, token);
                if (drive.Status == LtfsStatus.LTFS_INCONSISTENT
                    || drive.Status == LtfsStatus.LTFS_UNFORMATTED
                    || drive.Status == LtfsStatus.LTFS_MEDIA)
                {
                    return (LibraryOperationStatus.Succeeded, "Already loaded");
                }
            }

            var rt = (foundStatus ??
                (rs.ExitCode == 0 ? LibraryOperationStatus.Succeeded
                : LibraryOperationStatus.Failed), foundMessage ?? rs.Output);

            Log(traceId, $"Mount drive {drive.DeviceName} status: {rt.Item1} {rt.Item2 ?? ""}");
            return rt;
        }

        protected override async Task<(LibraryOperationStatus Status, string? Message)> LtfsUnmountAsync(Drive drive, string traceId, CancellationToken token)
        {
            Log(traceId, $"Unmounting drive {drive.DeviceName}...");

            var ltfsUnload = Options.LtfsPath == null ? "LtfsCmdEject" : Path.Combine(Options.LtfsPath, "LtfsCmdEject");
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var foundStatuses = new HashSet<LibraryOperationStatus>();
            var rs = await ExecAsync(ltfsUnload, drive.MountPoint!, traceId, token, message =>
            {
                if (TryFindLtfsKnownMessage(message, out var s))
                {
                    foundMessage = message;
                    if (s.HasValue)
                    {
                        foundStatus = s;
                        foundStatuses.Add(s.Value);
                    }
                }
            });

            if (foundStatus == LibraryOperationStatus.LTFS60233E)
            {
                await UpdateLtfsStatusAsync(traceId, token);
                if (drive.Status == LtfsStatus.NO_MEDIA)
                {
                    return (LibraryOperationStatus.Succeeded, "Already unmounted");
                }
            }
            else if (foundStatus == LibraryOperationStatus.LTFS12035E)// have to check the tape status if the drive release failed
            {
                foundStatus = LibraryOperationStatus.Succeeded;
                foundMessage = $"The drive {drive.DeviceName} or the tape {drive.LoadedMedia!.VolumeTag} is damaged";
                //if (foundStatuses.Contains(LibraryOperationStatus.LTFS10004E)
                //    || foundStatuses.Contains(LibraryOperationStatus.LTFS12012E))
                //{
                //    foundStatus = LibraryOperationStatus.Succeeded;
                //    foundMessage = $"The drive {drive.DeviceName} maybe damaged";
                //}
                //else
                //{
                //    var (ltfsck, ltfsckMsg) = await LtfsckAsync(drive.DeviceName, traceId, token);
                //    // Tape damaged, continue to unload the tape
                //    if (ltfsck.IsEjectable())
                //    {
                //        foundStatus = LibraryOperationStatus.Succeeded;
                //        foundMessage = $"The drive {drive.DeviceName} or the tape {drive.LoadedMedia!.VolumeTag} is damaged";
                //    }
                //}
            }

            var rt = (foundStatus ??
                (rs.ExitCode == 0 ? LibraryOperationStatus.Succeeded
                : LibraryOperationStatus.Failed), foundMessage ?? rs.Output);

            Log(traceId, $"Unmount drive {drive.DeviceName} status: {rt.Item1} {rt.Item2 ?? ""}");

            if (rt.Item1 == LibraryOperationStatus.Succeeded)
            {
                drive.Release();
            }

            return rt;
        }

        protected async Task<(LibraryOperationStatus Status, string? Message)> UnmountThenMountAsync(Drive drive, string traceId, CancellationToken token)
        {
            var (status, message) = await LtfsUnmountAsync(drive, traceId, token);
            if (status != LibraryOperationStatus.LTFS11034I && status != LibraryOperationStatus.Succeeded)
            {
                return (status, message);
            }

            await WaitAsync(5, token, traceId);

            (status, message) = await LtfsMountAsync(drive, traceId, token);

            if (status == LibraryOperationStatus.LTFS11031I && status != LibraryOperationStatus.Succeeded)
            {
                var driveInfo = new DriveInfo(drive.MountPoint!);
                if (driveInfo != null)
                {
                    drive.SetDriveInfo(driveInfo);
                }
            }
            return (status, message);
        }

        protected override async Task<LibraryOperation> DoMountInternalAsync(Drive drive, string traceId, CancellationToken token)
        {
            try
            {
                Log(traceId, $"Begin mount process for drive {drive.DeviceName}");

                await UpdateLtfsStatusAsync(traceId, token);

                if (!drive.IsAssigned)
                {
                    Log(traceId, $"Try assign a drive letter for {drive.DeviceName}");
                    var assign = await AssignAsync(drive, token);
                    if (assign.Status != LibraryOperationStatus.Succeeded)
                    {
                        return assign;
                    }
                    await WaitAsync(5, token, traceId, "Waiting for {0} seconds before check ltfs status");
                    await UpdateLtfsStatusAsync(traceId, token);
                }

                while (drive.Status == LtfsStatus.MEDIA_NOT_READY)
                {
                    await WaitAsync(10, token, traceId, "Ltfs status is MEDIA_NOT_READY. Waiting for {0} seconds before refresh status");
                    await UpdateLtfsStatusAsync(traceId, token);
                }

                if (drive.Status == LtfsStatus.LTFS_INCONSISTENT)
                {
                    var (status, message) = await LtfsckAsync(drive.DeviceName, traceId, token);
                    return (await VerifyLtfsckAsync(drive, status, message, traceId, token)).WaitBeforeNext(TimeSpan.FromSeconds(15));
                }
                if (drive.Status == LtfsStatus.LTFS_UNFORMATTED)
                {
                    return new LibraryOperation(LibraryOperationStatus.LTFS17168E, $"Drive {drive.SlotNumber} is unformatted");
                }
                if (drive.Status == LtfsStatus.NO_MEDIA && drive.IsFull)
                {
                    var (status, message) = await LtfsMountAsync(drive, traceId, token);
                    if (status != LibraryOperationStatus.Succeeded
                        && status != LibraryOperationStatus.LTFS11031I)
                    {
                        return await HandleCommonLtfsStatusAsync(drive, status, message);
                    }
                    await WaitAsync(5, token, traceId);
                    await UpdateLtfsStatusAsync(traceId, token);
                }

                return await HandleCommonLtfsStatusAsync(drive, default, $"Drive {drive.SlotNumber} has status {drive.Status}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while mount drive {drive.SlotNumber} {ex.Message} {ex.StackTrace}");
                return LibraryOperation.Fail($"Error while mount drive {drive.SlotNumber} {ex.Message}");
            }
        }

        protected override async Task<LibraryOperation> VerifyLtfsckAsync(Drive drive, LibraryOperationStatus ltfsStatus, string? ltfsMessage, string traceId, CancellationToken token)
        {
            try
            {
                Log(traceId, $"Verifying ltfsck result for drive {drive.DeviceName}. status: {ltfsStatus}, message: {ltfsMessage ?? ""}");

                await UpdateLtfsStatusAsync(traceId, token);

                // volume is consistent but need to remount
                if (ltfsStatus == LibraryOperationStatus.LTFS16022I
                    && drive.Status == LtfsStatus.LTFS_INCONSISTENT)
                {
                    var (status, message) = await UnmountThenMountAsync(drive, traceId, token);
                    return await HandleCommonLtfsStatusAsync(drive, ltfsStatus, ltfsMessage);
                }

                return await HandleCommonLtfsStatusAsync(drive, ltfsStatus, ltfsMessage);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while verifying ltfsck result on drive {drive.SlotNumber} {ex.Message} {ex.StackTrace}");
                return LibraryOperation.Fail($"Error while verifying ltfsck result on drive {drive.SlotNumber} {ltfsMessage ?? ""} {ex.Message}");
            }
        }

        protected override async Task<LibraryOperation> VerifyMkltfsAsync(Drive drive, LibraryOperationStatus ltfsStatus, string? ltfsMessage, string traceId, CancellationToken token)
        {
            try
            {
                Log(traceId, $"Verifying mkltfs result for drive {drive.DeviceName}. status: {ltfsStatus}, message: {ltfsMessage ?? ""}");

                await UpdateLtfsStatusAsync(traceId, token);

                return await HandleCommonLtfsStatusAsync(drive, ltfsStatus, ltfsMessage);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while verifying ltfsck result on drive {drive.SlotNumber} {ex.Message} {ex.StackTrace}");
                return LibraryOperation.Fail($"Error while verifying ltfsck result on drive {drive.SlotNumber} {ltfsMessage ?? ""} {ex.Message}");
            }
        }


        protected override async Task<LibraryOperation> DoUnmountThenUnloadAsync(Drive drive, string traceId, CancellationToken token)
        {
            var rs = await base.DoUnmountThenUnloadAsync(drive, traceId, token);
            if (rs.Succeeded)
            {
                var unassign = await LtfsUnassignAsync(traceId, drive.MountPoint!);
                if (unassign.Status != LibraryOperationStatus.Succeeded)
                {
                    return LibraryOperation.Fail($"Error while unassigning drive {drive.SlotNumber} from {drive.MountPoint} {unassign.Message ?? ""}");
                }
                await UpdateLtfsStatusAsync(traceId, token);
            }
            return rs;
        }
        #endregion
    }
}

using System.Text.RegularExpressions;
using Juice.Locks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace QualstarLibrary.Services.Linux
{
    internal class Library : LibraryBase
    {
        private IOptionsMonitor<LibraryOptions> _options;

        protected override Services.LibraryOptions Options => _options.CurrentValue;

        public Library(ILoggerFactory logger, IDistributedLock locker, IOptionsMonitor<LibraryOptions> options,
            IOperationRepository? operationRepository = null) : base(logger.CreateLogger("QualstarLibrary.Linux"), locker, operationRepository)
        {
            _options = options;
        }

        public override async Task<bool> IsReadyAsync(CancellationToken token)
        {
            // must collect changer first
            try
            {
                InitDrives();

                await CollectDevicesAsync(token);

                if (Drives.Any(d => d.DeviceName.Equals(d.Address)))
                {
                    await CollectDevNameAsync(token);
                }
                if (Drives.Any(d => d.DeviceName.Equals(d.Address)))
                {
                    _logger.LogWarning("Device name is not set for some drives, please check the configuration");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error initializing the library");
                return false;
            }
            if (await base.IsReadyAsync(token))
            {
                await CollectStatusAsync(true, token);
                var filesystems = await GetLtfsFilesystemsAsync(token);
                foreach (var drive in Drives)
                {
                    if (drive.IsFull)
                    {
                        var filesystem = filesystems.FirstOrDefault(fs => fs.Name.Equals(drive.DeviceName, StringComparison.OrdinalIgnoreCase));
                        if (filesystem != null)
                        {
                            drive.SetSizeInfo(filesystem.Size, filesystem.Avail);
                            drive.SetStatus(LtfsStatus.LTFS_MEDIA);
                            drive.AssignedTo(filesystem.MountedOn);
                        }
                        else
                        {
                            drive.SetStatus(LtfsStatus.NO_MEDIA);
                        }
                    }
                }
                return true;
            }
            return false;
        }

        /// <summary>
        /// Try umount all ltfs filesystems to get the ltfs device list
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        private async Task UmountAllAsync(CancellationToken token)
        {
            var filesystems = await GetLtfsFilesystemsAsync(token);

            foreach (var fs in filesystems)
            {
                var umount = await ExecAsync("umount", fs.MountedOn, default, token);
                if (umount.ExitCode != 0)
                {
                    _logger.LogWarning($"Failed to umount {fs.MountedOn}");
                }
            }
        }

        private async Task CollectDevNameAsync(CancellationToken token)
        {
            await UmountAllAsync(token);
            await Task.Delay(TimeSpan.FromSeconds(5), token);
            string pattern = @"Device Name =[\s]*(?<devname>[\S]+)[\s]+\((?<address>[\.0-9]+)\)[^\n]+Serial Number =[\s]*(?<serial>[^,]+)";
            var args = "-o device_list";
            var ltfs = Options.LtfsPath == null ? "ltfs" : Path.Combine(Options.LtfsPath, "ltfs");
            var rs = await ExecAsync(ltfs, args, default, token, (line) =>
            {
                var match = Regex.Match(line, pattern);
                if (match.Success)
                {
                    var devname = match.Groups["devname"].Value;
                    var address = match.Groups["address"].Value;
                    var serial = match.Groups["serial"].Value;
                    var drive = Drives.FirstOrDefault(d => d.Address.Equals(address, StringComparison.OrdinalIgnoreCase));
                    if (drive != null)
                    {
                        drive.SetDeviceName(devname);
                        drive.SetSerial(serial);
                    }
                    else
                    {
                        _logger.LogWarning($"Drive {address} is not configured");
                    }
                }
            });
            if (rs.ExitCode != 0)
            {
                _logger.LogWarning("Error getting devices name: {output}", rs.Output);
            }
        }

        private async Task CollectDevicesAsync(CancellationToken token)
        {
            var args = "/dev/sg -l";
            string pattern = @"(?<type>[Tape|Changer]+)-(([\S]*_(?<serial>[0-9A-Za-z]+))|[\S]+)[\s]+->[\s]+..\/(?<assigned>[^\n]+)";
            var rs = await ExecAsync("ls", args, default, token, (line) =>
            {
                var match = Regex.Match(line, pattern);
                if (match.Success)
                {
                    var type = match.Groups["type"].Value;
                    var serial = match.Groups["serial"].Value;
                    var assigned = match.Groups["assigned"].Value;

                    if (type.Equals("Changer", StringComparison.OrdinalIgnoreCase))
                    {
                        MtxChanger = Path.Combine("/dev", assigned);
                    }
                    else if (type.Equals("Tape", StringComparison.OrdinalIgnoreCase))
                    {
                        var drive = Drives.Where(d => d.Serial?.Equals(serial, StringComparison.OrdinalIgnoreCase) ?? false).FirstOrDefault();
                        drive?.SetDeviceName("/dev/" + match.Groups["assigned"].Value);
                    }
                }
            });
        }

        string mountPattern = @"ltfs:(?<dev>[\S]+)[\s]+(?<size>[\S]+)[\s]+(?<avail>[\S]+)[\s]+(?<mountedOn>[\S]+)";
        private async Task<Filesystem[]> GetLtfsFilesystemsAsync(CancellationToken token)
        {
            var rs = new List<Filesystem>();
            var args = "-h --output=source,size,avail,target";

            var p = await ExecAsync("df", args, default, token, (line) =>
            {
                var match = Regex.Match(line, mountPattern);
                if (match.Success)
                {
                    var dev = match.Groups["dev"].Value;
                    var size = SizeToB(match.Groups["size"].Value);
                    var avail = SizeToB(match.Groups["avail"].Value);
                    var mountedOn = match.Groups["mountedOn"].Value;
                    rs.Add(new Filesystem(dev, size, avail, mountedOn));
                }
            }, LogLevel.Debug);

            return rs.ToArray();
        }

        private long SizeToB(string size)
        {
            if (string.IsNullOrEmpty(size))
            {
                return default;
            }
            if (size.EndsWith("T"))
            {
                return (long)float.Parse(size.Substring(0, size.Length - 1)) * 1024 * 1024 * 1024 * 1024;
            }
            if (size.EndsWith("G"))
            {
                return (long)float.Parse(size.Substring(0, size.Length - 1)) * 1024 * 1024 * 1024;
            }
            if (size.EndsWith("M"))
            {
                return (long)float.Parse(size.Substring(0, size.Length - 1)) * 1024 * 1024;
            }
            if (long.TryParse(size, out var blocks))
            {
                return blocks * 1024;
            }
            return default;
        }

        protected override async Task<(LibraryOperationStatus Status, string? Message)> LtfsMountAsync(Drive drive,
            string traceId, CancellationToken token)
        {
            Log(traceId, $"Mounting drive {drive.DeviceName}...");

            var mountPoint = Path.Combine(_options.CurrentValue.MountPoint, "drive" + drive.SlotNumber);
            if (!Directory.Exists(mountPoint))
            {
                Directory.CreateDirectory(mountPoint);
            }
            var devName = drive.DeviceName;

            var filesystems = await GetLtfsFilesystemsAsync(token);
            if (filesystems.Any(fs => fs.MountedOn.Equals(mountPoint, StringComparison.OrdinalIgnoreCase)
                && fs.Name.Equals(devName, StringComparison.OrdinalIgnoreCase)
            ))
            {
                return (LibraryOperationStatus.Succeeded, null);
            }

            var args = $"-o devname={devName} {mountPoint}";
            var ltfs = Options.LtfsPath == null ? "ltfs" : Path.Combine(Options.LtfsPath, "ltfs");
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var rs = await ExecAsync(ltfs, args, traceId, token, message =>
            {
                if (TryFindLtfsKnownMessage(message, out var s))
                {
                    foundMessage = message;
                    foundStatus = s;
                }
            });
            var rt = (foundStatus ??
                (rs.ExitCode == 0 ? LibraryOperationStatus.Succeeded
                : LibraryOperationStatus.Failed), foundMessage ?? rs.Output);

            Log(traceId, $"Mount drive {drive.DeviceName} status: {rt.Item1} {rt.Item2 ?? ""}");
            if (rt.Item1.IsSuccess())
            {
                drive.AssignedTo(mountPoint);
            }
            return rt;
        }

        protected override async Task<(LibraryOperationStatus Status, string? Message)> LtfsUnmountAsync(Drive drive, string traceId, CancellationToken token)
        {
            Log(traceId, $"Unmounting drive {drive.DeviceName}...");

            var unmouted = false;
            var filesystems = await GetLtfsFilesystemsAsync(token);
            if (!filesystems.Any(fs => fs.MountedOn.Equals(drive.MountPoint, StringComparison.OrdinalIgnoreCase)
                && fs.Name.Equals(drive.DeviceName, StringComparison.OrdinalIgnoreCase)
            ))
            {
                _logger.LogInformation("The drive {device} is not mounted", drive.DeviceName);
                unmouted = true;
            }
            else
            {
                _logger.LogInformation("Unmounting the drive {device}", drive.DeviceName);
                var umount = await ExecAsync("umount", drive.MountPoint!, traceId, token);
                if (umount.ExitCode != 0)
                {
                    // try to check the mounted ltfs after 5 seconds
                    await WaitAsync(5, token, traceId);
                    filesystems = await GetLtfsFilesystemsAsync(token);
                    if (filesystems.Any(fs => fs.MountedOn.Equals(drive.MountPoint, StringComparison.OrdinalIgnoreCase)
                && fs.Name.Equals(drive.DeviceName, StringComparison.OrdinalIgnoreCase)))
                    {
                        return (LibraryOperationStatus.Failed, umount.Output);
                    }
                }
                unmouted = true;
                _logger.LogInformation("The drive {device} is unmounted", drive.DeviceName);

                await WaitAsync(5, token, traceId);

            }

            if (!unmouted)
            {
                return (LibraryOperationStatus.Failed, "Failed to unmount the drive");
            }
            drive.Unassigned();
            if (drive.IsReleased)
            {
                return (LibraryOperationStatus.Succeeded, "The drive is already released");
            }
            Log(traceId, $"Releasing the drive {drive.DeviceName}");

            var args = $"-o devname={drive.DeviceName} -o release_device";
            var ltfs = Options.LtfsPath == null ? "ltfs" : Path.Combine(Options.LtfsPath, "ltfs");
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var foundStatuses = new HashSet<LibraryOperationStatus>();
            var rs = await ExecAsync(ltfs, args, traceId, token, message =>
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

            // have to check the tape status if the drive release failed
            if (foundStatus == LibraryOperationStatus.LTFS12035E)
            {
                foundStatus = LibraryOperationStatus.Succeeded;
                foundMessage = $"The drive {drive.DeviceName} or the tape {drive.LoadedMedia!.VolumeTag} maybe damaged";
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
                //        foundMessage = $"The drive {drive.DeviceName} or the tape {drive.LoadedMedia!.VolumeTag} maybe damaged";
                //    }
                //}
            }

            var rt = (foundStatus ??
                (rs.ExitCode == 0 ? LibraryOperationStatus.Succeeded
                : LibraryOperationStatus.Failed), foundMessage ?? rs.Output);

            if (rt.Item1 == LibraryOperationStatus.Succeeded)
            {
                drive.Release();
            }

            Log(traceId, $"Unmount drive {drive.DeviceName} status: {rt.Item1} {rt.Item2 ?? ""}");

            return rt;

        }

        protected override async Task<LibraryOperation> DoMountInternalAsync(Drive drive, string traceId, CancellationToken token)
        {
            Log(traceId, $"Begin mount process for drive {drive.DeviceName}");

            var (status, message) = await LtfsMountAsync(drive, traceId, token);

            if (status == LibraryOperationStatus.LTFS16087E || status == LibraryOperationStatus.LTFS16021E)
            {
                await WaitAsync(10, token, traceId, "Waiting for {0} seconds before ltfsck");
                (status, message) = await LtfsckAsync(drive.DeviceName, traceId, token);
                if (status != LibraryOperationStatus.LTFS16022I)
                {
                    return (await VerifyLtfsckAsync(drive, status, message, traceId, token)).WaitBeforeNext(TimeSpan.FromSeconds(15));
                }
                await WaitAsync(5, token, traceId, "Waiting for {0} seconds before mount again");
                (status, message) = await LtfsMountAsync(drive, traceId, token);
            }

            return await HandleCommonLtfsStatusAsync(drive, status, message);
        }

        protected override async Task<LibraryOperation> VerifyLtfsckAsync(Drive drive, LibraryOperationStatus ltfsStatus,
            string? ltfsMessage, string traceId, CancellationToken token)
        {
            try
            {
                Log(traceId, $"Verifying ltfsck result for drive {drive.DeviceName}. status: {ltfsStatus}, message: {ltfsMessage ?? ""}");
                if (ltfsStatus != LibraryOperationStatus.LTFS16022I)
                {
                    return await HandleCommonLtfsStatusAsync(drive, ltfsStatus, ltfsMessage);
                }

                return new LibraryOperation(ltfsStatus, $"Tape {drive.LoadedMedia!.VolumeTag} is consistent");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while verifying ltfsck result on drive {drive.SlotNumber} {ex.Message} {ex.StackTrace}");
                return LibraryOperation.Fail($"Error while verifying ltfsck result on drive {drive.SlotNumber} {ltfsMessage ?? ""}");
            }
        }

        protected override async Task<LibraryOperation> VerifyMkltfsAsync(Drive drive, LibraryOperationStatus ltfsStatus, string? ltfsMessage, string traceId, CancellationToken token)
        {
            try
            {
                Log(traceId, $"Verifying mkltfs result for drive {drive.DeviceName}. status: {ltfsStatus}, message: {ltfsMessage ?? ""}");

                return await HandleCommonLtfsStatusAsync(drive, ltfsStatus, ltfsMessage);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while verifying ltfsck result on drive {drive.SlotNumber} {ex.Message} {ex.StackTrace}");
                return LibraryOperation.Fail($"Error while verifying ltfsck result on drive {drive.SlotNumber}");
            }
        }
    }
}

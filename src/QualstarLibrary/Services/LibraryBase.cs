using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Juice.Locks;
using Microsoft.Extensions.Logging;

namespace QualstarLibrary.Services
{
    internal abstract class LibraryBase : ILibrary
    {
        private object _lock = new object();
        private Drive[] _drives = new Drive[0];
        public Drive[] Drives
        {
            get
            {
                lock (_lock)
                {
                    return _drives;
                }
            }
            private set
            {
                lock (_lock)
                {
                    _drives = value;
                }
            }
        }

        private StorageSlot[] _slots = new StorageSlot[0];
        public StorageSlot[] Slots
        {
            get
            {
                lock (_lock)
                {
                    return _slots;
                }
            }
            private set
            {
                lock (_lock)
                {
                    _slots = value;
                }
            }
        }
        public ConcurrentDictionary<string, LibraryOperation> Operations { get; } = new ConcurrentDictionary<string, LibraryOperation>(StringComparer.OrdinalIgnoreCase);

        public event EventHandler<DriveEventArgs>? DriveChanged;
        public event EventHandler<MediaEventArgs>? MediaChanged;
        public event EventHandler<OperationLoggingEventArgs>? OperationLogging;

        protected IOperationRepository? OperationRepository { get; }
        protected ILogger _logger;
        protected abstract LibraryOptions Options { get; }
        protected IDistributedLock Locker;

        protected string? MtxChanger;

        public LibraryBase(ILogger logger, IDistributedLock locker, IOperationRepository? operationRepository)
        {
            _logger = logger;
            Locker = locker;
            OperationRepository = operationRepository;
        }

        #region Common
        protected void Log(string? operationId, string message, LogLevel level = LogLevel.Information)
        {
            OperationLogging?.Invoke(this, new OperationLoggingEventArgs(operationId, message));

            if (!string.IsNullOrEmpty(operationId) && Operations.TryGetValue(operationId, out var operation))
            {
                operation.AddLog(message);
            }
            if (_logger.IsEnabled(level))
            {
                _logger.Log(level, message);
            }
        }

        protected Task WaitAsync(int seconds, CancellationToken token, string? operationId, string message = "Waiting for {0} seconds...")
        {
            message = string.Format(message, seconds);
            if (!string.IsNullOrEmpty(operationId) && Operations.TryGetValue(operationId, out var operation))
            {
                operation.Wait(TimeSpan.FromSeconds(seconds));
                operation.AddLog(message);
                _logger.LogInformation(message);
            }
            return Task.Delay(TimeSpan.FromSeconds(seconds), token);
        }

        protected virtual async Task<ProcessResult> ExecAsync(string program, string args,
            string? traceId, CancellationToken token = default,
            Action<string>? logging = default, LogLevel level = LogLevel.Information)
        {
            var escapedArgs = args.Replace("\"", "\\\"");
            var startInfo = new ProcessStartInfo
            {
                FileName = program,
                Arguments = escapedArgs,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            var directory = Path.GetDirectoryName(program);
            using (_logger.BeginScope("Exec " + program + " " + (args ?? "")))
            {
                if (!string.IsNullOrEmpty(directory))
                {
                    startInfo.WorkingDirectory = directory;
                    _logger.LogInformation("Set process working directory {directory}", directory);
                }
                Log(traceId, $"Exec {program} {escapedArgs}", level);
                using var process = new Process()
                {
                    StartInfo = startInfo
                };
                var lastMessage = "";
                var dataEvt = new DataReceivedEventHandler((o, ee) =>
                {
                    if (ee?.Data != null)
                    {
                        logging?.Invoke(ee!.Data);
                        Log(traceId, ee.Data, level);
                        lastMessage = ee.Data;
                    }
                });
                process.OutputDataReceived += dataEvt;
                process.ErrorDataReceived += dataEvt;

                process.Start();

                process.BeginErrorReadLine();
                process.BeginOutputReadLine();

                var task = process.WaitForExitAsync(token);
                await task;
                if (task.IsCanceled)
                {
                    throw task.Exception ?? new Exception("Process was canceled");
                }
                var code = process.ExitCode;
                try
                {
                    var lastError = await process.StandardError.ReadToEndAsync();
                    var lastOutput = await process.StandardOutput.ReadToEndAsync();
                    if (!string.IsNullOrEmpty(lastError))
                    {
                        lastMessage = lastError;
                        logging?.Invoke(lastError);
                    }
                    if (!string.IsNullOrEmpty(lastOutput))
                    {
                        lastMessage = lastOutput;
                        logging?.Invoke(lastOutput);
                    }
                }
                catch (Exception)
                {
                }

                Log(traceId, $"Exit code: {code}. Last message: {lastMessage}", level);
                return new ProcessResult(code, lastMessage);
            }
        }

        private ConcurrentDictionary<uint, Task<LibraryOperation>> _driveTasks = new ConcurrentDictionary<uint, Task<LibraryOperation>>();

        protected LibraryOperation? IsDriveBusy(uint driveSlotNumber)
        {
            if (_driveTasks.TryGetValue(driveSlotNumber, out var task))
            {
                if (task.IsCompleted)
                {
                    var result = task.Result;
                    Operations[result.TraceId] = result;
                    if (_driveTasks.TryRemove(driveSlotNumber, out _))
                    {
                        return default;
                    }
                }

                return LibraryOperation.DriveBusy;
            }
            else
            {
                return default;
            }
        }

        protected async Task<LibraryOperation> OperationWrapperAsync(Task<LibraryOperation> operation, uint driveSlotNumber, string traceId, TimeSpan wait)
        {
            var busy = IsDriveBusy(driveSlotNumber);
            if (busy != default) { return busy; }

            _logger.LogInformation("Operation {traceId} is starting", traceId);
            Operations[traceId] = LibraryOperation.Ongoing(TimeSpan.FromSeconds(30))
    .SetTraceId(traceId);

            if (OperationRepository != null)
            {
                try
                {
                    OperationRepository.AddOperationAsync(Operations[traceId], default).Wait();
                }
                catch (Exception ex)
                {
                    _logger.LogError("Failed to save operation {traceId} {message} {trace}", traceId, ex.Message, ex.StackTrace);
                }
            }

            var task = operation.ContinueWith(t =>
            {
                return Operations.AddOrUpdate(traceId,
                    t.IsFaulted
                    ? LibraryOperation.Fail(t.Exception!.InnerException!.Message).SetTraceId(traceId)
                    : t.Result.SetTraceId(traceId),
                    (k, v) =>
                    {
                        if (t.IsFaulted)
                        {
                            _logger.LogInformation("Operation {traceId} finished with status {satus} {message}", traceId, LibraryOperationStatus.Failed, t.Exception!.InnerException!.Message);
                            v.SetStatus(LibraryOperationStatus.Failed, t.Exception!.InnerException!.Message);
                        }
                        else
                        {
                            _logger.LogInformation("Operation {traceId} finished with status {satus} {message}", traceId, t.Result.Status, t.Result.Message);
                            v.SetStatus(t.Result.Status, t.Result.Message);
                            if (t.Result.WaitBeforeNextOperation.HasValue)
                            {
                                v.WaitBeforeNext(t.Result.WaitBeforeNextOperation.Value);
                            }
                        }
                        if (OperationRepository != null)
                        {
                            try
                            {
                                OperationRepository.UpdateOrAddOperationAsync(v, default).Wait();
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError("Failed to save operation {traceId} {message} {trace}", traceId, ex.Message, ex.StackTrace);
                            }
                        }
                        return v;
                    });
            });
            _driveTasks[driveSlotNumber] = task;

            if (await Task.WhenAny(task, Task.Delay(wait)) == task)
            {
                return await task;
            }
            else
            {
                return Operations[traceId];
            }
        }


        /// <summary>
        /// Try to lock drive for operation in OperationLockTime option
        /// </summary>
        /// <param name="slotNumber"></param>
        /// <param name="operationId"></param>
        /// <param name="operation"></param>
        /// <param name="expiration"></param>
        /// <returns></returns>
        protected virtual async Task<ILock?> LockDriveAsync(uint slotNumber, string operationId, string? operation, TimeSpan expiration)
        {
            // should generate unique id for each operation to avoid deadlocks and duplicated lock
            var @lock = await Locker.AcquireLockAsync($"TapeDrive-{slotNumber}", operationId, expiration);
            if (@lock != default)
            {
                var drive = Drives.Where(d => d.SlotNumber == slotNumber).FirstOrDefault();

                Trigger(DriveChanged, new DriveEventArgs(slotNumber, operation != null ?
                    $"Locked to {operation}" : "Locked"));

                @lock.Released += (sender, e) =>
                {
                    Trigger(DriveChanged, new DriveEventArgs(slotNumber, operation != null ? $"Unlocked after {operation}" : "Unlocked"));
                };
            }
            return @lock;
        }

        protected virtual async Task<ILock?> LockChangerAsync(string operationId, TimeSpan? expiration = null)
        {
            var @lock = await Locker.AcquireLockAsync($"TapeChanger",
                operationId,
                expiration ?? TimeSpan.FromMinutes(2));

            return @lock;
        }

        protected void Trigger<TEvent>(EventHandler<TEvent>? eventHandler, TEvent e)
        {
            if (eventHandler != null)
            {
                try
                {
                    var @event = eventHandler;
                    @event(this, e);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while handling {event} event", typeof(TEvent).Name);
                }
            }
        }

        #endregion

        #region Init
        public virtual async Task<bool> IsReadyAsync(CancellationToken token)
        {
            if (!Drives.Any() || !Slots.Any())
            {
                try
                {
                    InitDrives();
                    await InitMtxAsync(token);
                }
                catch (Exception ex)
                {
                    _logger.LogError("Failed to init library {message} {trace}", ex.Message, ex.StackTrace);
                    return false;
                }
            }
            return true;
        }

        #endregion

        #region Mtx
        protected virtual async Task<ProcessResult> MtxAsync(string args, string? traceId,
            CancellationToken token = default,
            Action<string>? logging = default)
        {
            var mtx = Options.MtxPath == null ? "mtx"
                : Path.Combine(Options.MtxPath, "mtx");
            var logLevel = args == "status" ? LogLevel.Debug : LogLevel.Information;
            if (!string.IsNullOrEmpty(MtxChanger))
            {
                args = $"-f {MtxChanger} {args}";
            }
            return await ExecAsync(mtx, args, traceId, token, logging, logLevel);
        }

        /// <summary>
        /// Data Transfer Element 0:Empty
        /// Data Transfer Element 1:Full(Storage Element 27 Loaded) :VolumeTag = 000063L7
        /// Storage Element 400:Empty
        /// Storage Element 436 IMPORT/EXPORT:Full :VolumeTag=000063L7  
        /// Storage Element 437 IMPORT/EXPORT:Empty
        /// </summary>
        /// 

        private Regex _elementRegex = new Regex(@"(?<type>Storage|Data Transfer) Element[\s]+(?<slot>[0-9]+)[\s]*(?<io>IMPORT\/EXPORT)*:(?<status>[^\s]+)([\s]+(\(Storage Element (?<loadedSlot>[0-9]+) Loaded\))*:VolumeTag[\s]*=[\s]*(?<tag>[^\s]+))*", RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        protected void InitDrives()
        {
            if (!Drives.Any())
            {
                if (!Options.Drives.Any())
                {
                    throw new Exception("No drives configured!");
                }

                Drives = Options.Drives.Select(d => new Drive(d.SlotNumber, d.Address, d.Serial)).ToArray();
            }
        }

        private async Task InitMtxAsync(CancellationToken token)
        {
            var slots = new List<StorageSlot>();
            bool notReady = false;
            var result = await MtxAsync("status", default, token, (line) =>
            {
                if (line.Contains("Sense Key=Not Ready", StringComparison.OrdinalIgnoreCase))
                {
                    notReady = true;
                }
                var match = _elementRegex.Match(line);
                if (match.Success)
                {
                    var type = match.Groups["type"].Value;
                    var slot = uint.Parse(match.Groups["slot"].Value);
                    var io = match.Groups["io"].Value;
                    var status = match.Groups["status"].Value;
                    var loadedSlot = match.Groups["loadedSlot"].Value;
                    var tag = match.Groups["tag"].Value;
                    if (type.Equals("Data Transfer", StringComparison.OrdinalIgnoreCase))
                    {
                        UpdateDataTransferElement(slot, status, loadedSlot, tag);
                    }
                    else if (type.Equals("Storage", StringComparison.OrdinalIgnoreCase))
                    {
                        slots.Add(CreateStorageElement(slot, status, io != null, tag));
                    }
                }
            });
            Slots = slots.ToArray();
            if (slots.Any() && notReady)
            {
                throw new Exception("Library not ready!");
            }
            if (result.ExitCode != 0)
            {
                throw new Exception(result.Output);
            }
        }

        protected virtual async Task UpdateMtxStatusAsync(CancellationToken token)
        {
            var result = await MtxAsync("status", default, default, (line) =>
            {
                var match = _elementRegex.Match(line);
                if (match.Success)
                {
                    var type = match.Groups["type"].Value;
                    var slot = uint.Parse(match.Groups["slot"].Value);
                    var io = match.Groups["io"].Value;
                    var status = match.Groups["status"].Value;
                    var loadedSlot = match.Groups["loadedSlot"].Value;
                    var tag = match.Groups["tag"].Value;
                    if (type.Equals("Data Transfer", StringComparison.OrdinalIgnoreCase))
                    {
                        UpdateDataTransferElement(slot, status, loadedSlot, tag);
                    }
                    else if (type.Equals("Storage", StringComparison.OrdinalIgnoreCase))
                    {
                        UpdateStorageElement(slot, status, io != null, tag);
                    }
                }

            });
            if (result.ExitCode != 0)
            {
                throw new Exception(result.Output);
            }
        }

        protected virtual void UpdateDataTransferElement(uint slotNumber, string status, string? loadedSlot, string? tag)
        {

            lock (_lock)
            {
                var drive = Drives.Where(d => d.SlotNumber == slotNumber).FirstOrDefault();
                if (drive == null)
                {
                    _logger.LogWarning("Drive {slot} not configured", slotNumber);
                    return;
                }

                if (status.Equals("Full", StringComparison.OrdinalIgnoreCase))
                {
                    if (!string.IsNullOrEmpty(tag) && !drive.IsLoadedMedia(tag))
                    {
                        var slot = Slots.Where(s => s.IsLoadedMedia(tag)).FirstOrDefault();
                        var media = slot?.Media ??
                            (loadedSlot != null
                                ? new Media(tag, uint.Parse(loadedSlot))
                                : new Media(tag)
                            );
                        slot?.Empty();
                        drive.LoadMedia(media);
                    }
                }
                else
                {
                    drive.UnloadMedia();
                }
                _logger.LogInformation("Matched drive {slot} {status} {tag} {loadedSlot}", drive.SlotNumber,
                    status, tag, loadedSlot);
            }
        }

        protected virtual void UpdateStorageElement(uint slotNumber, string status, bool io, string? tag)
        {

            lock (_lock)
            {
                var slot = Slots.Where(s => s.SlotNumber == slotNumber).FirstOrDefault();
                if (slot == null)
                {
                    _logger.LogInformation("Slot {slot} not registered", slotNumber);
                    return;
                }
                if (status.Equals("Full", StringComparison.OrdinalIgnoreCase))
                {
                    if (!string.IsNullOrEmpty(tag))
                    {
                        if (!slot.IsLoadedMedia(tag))
                        {
                            slot.StoredMedia(new Media(tag));
                        }

                        if (Drives.Any(d => d.IsLoadedMedia(tag)))
                        {
                            _logger.LogWarning("The tape with tag {tape} found in drive", tag);
                        }
                        if (Slots.Any(s => s.IsLoadedMedia(tag) && s.SlotNumber < slotNumber))
                        {
                            _logger.LogWarning("The tape with tag {tape} found in other slots", tag);
                        }
                    }
                }
                else
                {
                    slot.Empty();
                }
            }
        }

        protected virtual StorageSlot CreateStorageElement(uint slotNumber, string status, bool io, string? tag)
        {
            var slot = new StorageSlot(slotNumber, io);
            if (!string.IsNullOrEmpty(tag))
            {
                slot.StoredMedia(new Media(tag));
            }
            return slot;
        }


        #endregion

        #region Ltfs
        /// <summary>
        /// Format a tape in drive
        /// </summary>
        /// <param name="devName"></param>
        /// <param name="tapeSerial"></param>
        /// <param name="force"></param>
        /// <param name="traceId"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        protected virtual async Task<(LibraryOperationStatus Status, string? Message)> MkltfsAsync(string devName, string? tapeSerial, bool force, string traceId, CancellationToken token)
        {
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var args = $"--device={devName}";
            if (!string.IsNullOrEmpty(tapeSerial))
            {
                if (tapeSerial.Length == 6)
                {
                    args += $" --tape-serial={tapeSerial}";
                }
                else
                {
                    Log(traceId, "Mkltfs: The tape serial number must be 6 characters long");
                }
            }
            if (force)
            {
                args += " --force";
            }
            var mkltfs = Options.LtfsPath == null ? "mkltfs" : Path.Combine(Options.LtfsPath, "mkltfs");
            var rs = await ExecAsync(mkltfs, args, traceId, token, message =>
            {
                if (TryFindLtfsKnownMessage(message, out var s))
                {
                    foundMessage = message;
                    foundStatus = s;
                }
            });

            return (foundStatus ?? LibraryOperationStatus.Failed, foundMessage ?? rs.Output);
        }

        protected virtual async Task<(LibraryOperationStatus Status, string? Message)> LtfsckAsync(string devName, string traceId, CancellationToken token)
        {
            string? foundMessage = null;
            LibraryOperationStatus? foundStatus = null;
            var ltfsck = Options.LtfsPath == null ? "ltfsck" : Path.Combine(Options.LtfsPath, "ltfsck");
            var rs = await ExecAsync(ltfsck, devName, traceId, token, message =>
            {
                if (TryFindLtfsKnownMessage(message, out var s))
                {
                    foundMessage = message;
                    foundStatus = s;
                }
            });

            return (foundStatus ?? LibraryOperationStatus.Failed, foundMessage ?? rs.Output);
        }

        protected abstract Task<(LibraryOperationStatus Status, string? Message)> LtfsMountAsync(Drive drive, string traceId, CancellationToken token);
        protected abstract Task<(LibraryOperationStatus Status, string? Message)> LtfsUnmountAsync(Drive drive, string traceId, CancellationToken token);

        private Regex _ltfsRegex = new(@"(?<status>LTFS\d{5}[EI]).*$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
        protected bool TryFindLtfsKnownMessage(string message, out LibraryOperationStatus? status)
        {
            status = null;
            if (string.IsNullOrEmpty(message))
            {
                return false;
            }

            var match = _ltfsRegex.Match(message);
            if (match.Success)
            {
                var statusStr = match.Groups["status"].Value;
                if (Enum.TryParse(statusStr, out LibraryOperationStatus parsedStatus))
                {
                    status = parsedStatus;
                    return true;
                }
            }
            return false;
        }

        protected virtual async Task<LibraryOperation> HandleCommonLtfsStatusAsync(Drive drive, LibraryOperationStatus? ltfsStatus, string? ltfsMessage)
        {
            await Task.Yield();
            switch (ltfsStatus)
            {
                case LibraryOperationStatus.LTFS11331E:
                case LibraryOperationStatus.LTFS11006E:
                case LibraryOperationStatus.LTFS12019E:
                    drive.SetStatus(LtfsStatus.NO_MEDIA);
                    drive.Failed();
                    Trigger(DriveChanged, new DriveEventArgs(drive.SlotNumber, "Failure"));
                    return new LibraryOperation(ltfsStatus.Value, $"The drive {drive.DeviceName} or the tape {drive.LoadedMedia!.VolumeTag} is damaged");
                case LibraryOperationStatus.LTFS17168E:
                    drive.SetStatus(LtfsStatus.LTFS_UNFORMATTED);
                    return new LibraryOperation(LibraryOperationStatus.LTFS17168E, $"Tape {drive.LoadedMedia!.VolumeTag} is unformatted");
                case LibraryOperationStatus.LTFS11095E:
                    drive.WriteProtected();
                    return new LibraryOperation(LibraryOperationStatus.LTFS11095E, $"The tape {drive.LoadedMedia!.VolumeTag} is write protected");
                case LibraryOperationStatus.LTFS16021E:
                case LibraryOperationStatus.LTFS16087E:
                    drive.SetStatus(LtfsStatus.LTFS_INCONSISTENT);
                    return new LibraryOperation(ltfsStatus.Value, $"The tape {drive.LoadedMedia!.VolumeTag} is inconsistent");
                case LibraryOperationStatus.LTFS15024I:
                case LibraryOperationStatus.LTFS11031I:
                    {
                        drive.SetStatus(LtfsStatus.LTFS_MEDIA);
                        var driveInfo = new DriveInfo(drive.MountPoint!);
                        if (driveInfo != null)
                        {
                            drive.SetDriveInfo(driveInfo);
                        }
                        Trigger(DriveChanged, new DriveEventArgs(drive.SlotNumber, "Mount"));
                        return new LibraryOperation(ltfsStatus.Value, ltfsMessage);
                    }
                default:
                    if (drive.Status == LtfsStatus.LTFS_UNFORMATTED)
                    {
                        return new LibraryOperation(LibraryOperationStatus.LTFS17168E, $"Tape {drive.LoadedMedia!.VolumeTag} is unformatted");
                    }
                    else if (drive.Status == LtfsStatus.LTFS_MEDIA)
                    {
                        var driveInfo = new DriveInfo(drive.MountPoint!);
                        if (driveInfo != null)
                        {
                            drive.SetDriveInfo(driveInfo);
                        }
                        Trigger(DriveChanged, new DriveEventArgs(drive.SlotNumber, "Mount"));
                        return new LibraryOperation(LibraryOperationStatus.LTFS11031I);
                    }
                    return new LibraryOperation(ltfsStatus ?? LibraryOperationStatus.Failed, ltfsMessage);
            }

        }

        #endregion

        DateTime _lastCollected = DateTime.MinValue;
        public virtual async Task CollectStatusAsync(bool force, CancellationToken token)
        {
            if (DateTime.Now - _lastCollected < TimeSpan.FromSeconds(15) && !force)
            {
                return;
            }
            _lastCollected = DateTime.Now;
            await UpdateMtxStatusAsync(token);
        }

        #region Format - mkltfs

        public virtual Task<LibraryOperation> FormatAsync(uint driveSlotNumber, bool force, CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            return OperationWrapperAsync(DoFormatAsync(driveSlotNumber, force, id, token), driveSlotNumber, id, TimeSpan.FromSeconds(15));
        }

        private async Task<LibraryOperation> DoFormatAsync(uint slotNumber, bool force, string traceId, CancellationToken token)
        {
            try
            {
                await UpdateMtxStatusAsync(token);
                var drive = Drives.FirstOrDefault(d => d.SlotNumber == slotNumber);
                if (drive == null)
                {
                    return LibraryOperation.DriveNotFound;
                }
                var volumeTag = drive.LoadedMedia?.VolumeTag;
                if (string.IsNullOrEmpty(volumeTag))
                {
                    throw new Exception($"Drive {drive.SlotNumber} is empty");
                }

                using var @lock = await LockDriveAsync(drive.SlotNumber, traceId, "format LTFS", TimeSpan.FromMinutes(5));
                if (@lock == null)
                {
                    return LibraryOperation.DriveBusy;
                }
                var (status, message) = await MkltfsAsync(drive.DeviceName, Utils.TapeSerial(volumeTag), force, traceId, token);
                return (await VerifyMkltfsAsync(drive, status, message, traceId, token))
                    .WaitBeforeNext(TimeSpan.FromSeconds(15));
            }
            catch (Exception ex)
            {
                return LibraryOperation.Fail(ex);
            }
        }

        protected abstract Task<LibraryOperation> VerifyMkltfsAsync(Drive drive, LibraryOperationStatus ltfsStatus, string? ltfsMessage, string traceId, CancellationToken token);
        #endregion

        #region Check - ltfsck
        public virtual Task<LibraryOperation> LtfsckAsync(uint driveSlotNumber, CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            return OperationWrapperAsync(DoLtfsckAsync(driveSlotNumber, id, token), driveSlotNumber, id, TimeSpan.FromSeconds(15));
        }

        private async Task<LibraryOperation> DoLtfsckAsync(uint slotNumber, string traceId, CancellationToken token)
        {
            try
            {
                var drive = Drives.FirstOrDefault(d => d.SlotNumber == slotNumber);
                if (drive == null)
                {
                    return LibraryOperation.DriveNotFound;
                }
                if (string.IsNullOrEmpty(drive.LoadedMedia?.VolumeTag))
                {
                    throw new Exception($"Drive {drive.SlotNumber} is empty");
                }
                using var @lock = await LockDriveAsync(drive.SlotNumber, traceId, "ltfsck", TimeSpan.FromMinutes(5));
                if (@lock == null)
                {
                    return LibraryOperation.DriveBusy;
                }

                var (status, message) = await LtfsckAsync(drive.DeviceName, traceId, token);

                if (status == LibraryOperationStatus.LTFS16022I)
                {
                    return (await VerifyLtfsckAsync(drive, status, message, traceId, token))
                        .WaitBeforeNext(TimeSpan.FromSeconds(15));
                }

                return new LibraryOperation(status, message);
            }
            catch (Exception ex)
            {
                return LibraryOperation.Fail(ex);
            }
        }

        protected abstract Task<LibraryOperation> VerifyLtfsckAsync(Drive drive, LibraryOperationStatus ltfsStatus, string? ltfsMessage, string traceId, CancellationToken token);
        #endregion

        #region Mount / load
        public virtual async Task<LibraryOperation> LoadAsync(string volumeTag, uint driveSlotNumber, CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            await CollectStatusAsync(true, token);
            var drive = Drives.FirstOrDefault(d => d.SlotNumber == driveSlotNumber);
            if (drive == null)
            {
                return LibraryOperation.DriveNotFound;
            }
            return await OperationWrapperAsync(DoLoadThenMountAsync(drive, volumeTag, id, token), driveSlotNumber, id, TimeSpan.FromSeconds(15));
        }

        private async Task<LibraryOperation> DoLoadThenMountAsync(Drive drive, string volumeTag, string traceId, CancellationToken token)
        {
            try
            {
                // drive is full and loaded with other tape
                if (!(drive.LoadedMedia?.VolumeTag?.Equals(volumeTag) ?? true))
                {
                    var rs = await DoUnmountThenUnloadAsync(drive, traceId, token);
                    if (!rs.Succeeded)
                    {
                        return rs;
                    }

                    await WaitAsync(500, token, traceId);
                    await UpdateMtxStatusAsync(token);
                }

                using var @lock = await LockDriveAsync(drive.SlotNumber, traceId, "mount LTFS", TimeSpan.FromMinutes(5));
                if (@lock == null)
                {
                    return LibraryOperation.DriveBusy;
                }

                if (!drive.IsFull)
                {
                    // Load tape into drive
                    var slot = Slots.FirstOrDefault(s => s.IsLoadedMedia(volumeTag));
                    if (slot == null)
                    {
                        return LibraryOperation.TapeNotFound;
                    }

                    Log(traceId, $"Loading tape {volumeTag} into drive {drive.SlotNumber}");
                    using (var @lock2 = await LockChangerAsync(traceId, TimeSpan.FromMinutes(5)))
                    {
                        if (@lock2 == null)
                        {
                            return LibraryOperation.MtxBusy;
                        }

                        var args = $"load {slot.SlotNumber} {drive.SlotNumber}";
                        var load = await MtxAsync(args, traceId, token);

                        if (load.ExitCode != 0)
                        {
                            // Failed to load tape into drive but need to verify if the tape is already loaded
                            await WaitAsync(10, token, traceId, $"Load return exit code {load.ExitCode}. Waiting for {{0}} seconds before manual check mtx status");
                            await UpdateMtxStatusAsync(token);

                            if (!(drive.LoadedMedia?.VolumeTag?.Equals(volumeTag) ?? false))
                            {
                                Trigger(DriveChanged, new DriveEventArgs(drive.SlotNumber, "Load"));
                                Trigger(MediaChanged, new MediaEventArgs(volumeTag));
                                return LibraryOperation.Fail(load.Output ?? $"Failed to load tape {volumeTag} into drive {drive.SlotNumber}");
                            }
                        }
                        else
                        {
                            await WaitAsync(5, token, traceId);
                            await UpdateMtxStatusAsync(token);
                        }
                    }
                }

                if (drive.LoadedMedia == null
                    || !volumeTag.Equals(drive.LoadedMedia.VolumeTag, StringComparison.OrdinalIgnoreCase))
                {
                    return LibraryOperation.Fail($"Failed to load tape {volumeTag} into drive {drive.SlotNumber}");
                }

                return await DoMountInternalAsync(drive!, traceId, token);
            }
            catch (Exception ex)
            {
                return LibraryOperation.Fail(ex);
            }
        }

        protected abstract Task<LibraryOperation> DoMountInternalAsync(Drive drive, string traceId, CancellationToken token);

        public virtual Task<LibraryOperation> MountAsync(uint driveSlotNumber, CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            return OperationWrapperAsync(DoMountAsync(driveSlotNumber, id, token), driveSlotNumber, id, TimeSpan.FromSeconds(15));
        }

        private async Task<LibraryOperation> DoMountAsync(uint slotNumber, string traceId, CancellationToken token)
        {
            try
            {
                await CollectStatusAsync(true, token);
                var drive = Drives.FirstOrDefault(d => d.SlotNumber == slotNumber);
                if (drive == null)
                {
                    return LibraryOperation.DriveNotFound;
                }
                if (!drive.IsFull)
                {
                    return LibraryOperation.TapeNotFound;
                }

                using var @lock = await LockDriveAsync(drive.SlotNumber, traceId, "mount LTFS", TimeSpan.FromMinutes(5));
                if (@lock == null)
                {
                    return LibraryOperation.DriveBusy;
                }

                return await DoMountInternalAsync(drive!, traceId, token);
            }
            catch (Exception ex)
            {
                return LibraryOperation.Fail(ex);
            }
        }

        #endregion

        #region Unmount / unload

        public virtual async Task<LibraryOperation> UnloadAsync(uint driveSlotNumber, CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            await CollectStatusAsync(true, token);
            var drive = Drives.FirstOrDefault(d => d.SlotNumber == driveSlotNumber);
            if (drive == null)
            {
                return LibraryOperation.DriveNotFound;
            }
            return await OperationWrapperAsync(DoUnmountThenUnloadAsync(drive, id, token), driveSlotNumber, id, TimeSpan.FromSeconds(15));
        }

        protected virtual async Task<LibraryOperation> DoUnmountThenUnloadAsync(Drive drive, string traceId, CancellationToken token)
        {
            try
            {
                // drive is empty
                if (!drive.IsFull)
                {
                    return LibraryOperation.Success;
                }
                await Task.Delay(3000, token);
                using var @lock = await LockDriveAsync(drive.SlotNumber, traceId, "umount LTFS", TimeSpan.FromMinutes(5));
                if (@lock == null)
                {
                    return LibraryOperation.DriveBusy;
                }

                if (drive.IsAssigned)
                {
                    var (status, message) = await LtfsUnmountAsync(drive, traceId, token);

                    if (status != LibraryOperationStatus.LTFS11034I
                        && status != LibraryOperationStatus.Succeeded)
                    {
                        return new LibraryOperation(status, message);
                    }
                    Trigger(DriveChanged, new DriveEventArgs(drive.SlotNumber, "Unmount"));
                    await WaitAsync(5, token, traceId);
                }

                Log(traceId, $"Unloading tape from drive {drive.SlotNumber}");
                using var @lock2 = await LockChangerAsync(traceId, TimeSpan.FromMinutes(5));

                if (@lock2 == null)
                {
                    return LibraryOperation.MtxBusy;
                }
                var volumeTag = drive.LoadedMedia!.VolumeTag;
                var storageSlotNumber = drive.LoadedSlotNumber;
                var args = $"unload {storageSlotNumber} {drive.SlotNumber}";
                var unload = await MtxAsync(args, traceId, token);
                if (unload.ExitCode != 0)
                {
                    @lock2.Dispose();
                    @lock.Dispose();
                    await WaitAsync(10, token, traceId, $"Unload return exit code {unload.ExitCode}. Waiting for {{0}} seconds before manual check mtx status");
                    await UpdateMtxStatusAsync(token);

                    var slot = Slots.FirstOrDefault(s => s.IsLoadedMedia(volumeTag));
                    if (slot?.Media?.VolumeTag?.Equals(volumeTag, StringComparison.OrdinalIgnoreCase) ?? false)
                    {
                        Trigger(DriveChanged, new DriveEventArgs(drive.SlotNumber, "Unload"));
                        Trigger(MediaChanged, new MediaEventArgs(volumeTag));
                        return LibraryOperation.Success;
                    }

                    return LibraryOperation.Fail(unload.Output ?? $"Failed to unload tape from drive {drive.SlotNumber} to storage {storageSlotNumber}");
                }

                return LibraryOperation.Success;
            }
            catch (Exception ex)
            {
                return LibraryOperation.Fail(ex);
            }
        }

        public virtual Task<LibraryOperation> UnmountAsync(uint driveSlotNumber, CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            return OperationWrapperAsync(DoUnmountAsync(driveSlotNumber, id, token), driveSlotNumber, id, TimeSpan.FromSeconds(15));
        }

        private async Task<LibraryOperation> DoUnmountAsync(uint slotNumber, string traceId, CancellationToken token)
        {
            try
            {
                await CollectStatusAsync(true, token);
                var drive = Drives.FirstOrDefault(d => d.SlotNumber == slotNumber);
                if (drive == null)
                {
                    return LibraryOperation.DriveNotFound;
                }
                // drive is empty
                if (!drive.IsAssigned)
                {
                    return LibraryOperation.Success;
                }
                using var @lock = await LockDriveAsync(drive.SlotNumber, traceId, "umount LTFS", TimeSpan.FromMinutes(5));
                if (@lock == null)
                {
                    return LibraryOperation.DriveBusy;
                }

                var (status, message) = await LtfsUnmountAsync(drive, traceId, token);

                return new LibraryOperation(status, message);

            }
            catch (Exception ex)
            {
                return LibraryOperation.Fail(ex);
            }
        }

        #endregion

        #region Transfer
        public virtual Task<LibraryOperation> TransferAsync(string volumeTag, uint targetStorageSlotNumber, CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            return OperationWrapperAsync(DoTransferAsync(volumeTag, targetStorageSlotNumber, id, token), targetStorageSlotNumber, id, TimeSpan.FromSeconds(15));
        }

        private async Task<LibraryOperation> DoTransferAsync(string volumeTag, uint slotNumber, string traceId, CancellationToken token)
        {
            try
            {
                await UpdateMtxStatusAsync(token);
                var slot = Slots.FirstOrDefault(s => s.SlotNumber == slotNumber);
                if (slot == null)
                {
                    throw new ArgumentException($"Slot {slotNumber} not found");
                }
                if (!(slot.Media?.VolumeTag?.Equals(volumeTag, StringComparison.OrdinalIgnoreCase) ?? true))
                {
                    throw new Exception($"Slot {slot.SlotNumber} is not empty");
                }
                var currentSlot = Slots.FirstOrDefault(s => s.IsLoadedMedia(volumeTag));
                if (currentSlot == null)
                {
                    return LibraryOperation.TapeNotFound;
                }
                Log(traceId, $"Transfering tape from slot {currentSlot.SlotNumber} to {slotNumber}");
                var transfer = await MtxAsync($"transfer {currentSlot.SlotNumber} {slotNumber}", traceId, token);
                if (transfer.ExitCode != 0)
                {
                    await WaitAsync(10, token, traceId, $"Transfer return exit code {transfer.ExitCode}. Waiting for {{0}} seconds before manual check mtx status");
                    await UpdateMtxStatusAsync(token);

                    if (slot?.Media?.VolumeTag.Equals(volumeTag, StringComparison.OrdinalIgnoreCase) ?? false)
                    {
                        Trigger(MediaChanged, new MediaEventArgs(volumeTag));
                        return LibraryOperation.Success;
                    }
                    return LibraryOperation.Fail(transfer.Output ?? $"Failed to transfer tape {volumeTag} from slot {currentSlot.SlotNumber} to slot {slotNumber}");
                }
                return LibraryOperation.Success;
            }
            catch (Exception ex)
            {
                return LibraryOperation.Fail(ex);
            }
        }
        #endregion

        #region Release
        public virtual Task<LibraryOperation> ReleaseAsync(CancellationToken token)
        {
            var id = Guid.NewGuid().ToString();
            return OperationWrapperAsync(DoReleaseAsync(id, token), 999, id, TimeSpan.FromSeconds(15));
        }

        protected virtual async Task<LibraryOperation> DoReleaseAsync(string traceId, CancellationToken token)
        {
            try
            {
                await CollectStatusAsync(true, token);
                foreach (var drive in Drives)
                {
                    if (drive.IsFull)
                    {
                        var rs = await DoUnmountThenUnloadAsync(drive, traceId, token);
                        if (!rs.Succeeded)
                        {
                            return rs;
                        }
                    }

                }
                return LibraryOperation.Success;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while releasing the library");
                return LibraryOperation.Fail(ex.Message);
            }
        }
        #endregion
    }
}

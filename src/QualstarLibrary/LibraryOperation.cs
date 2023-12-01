using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Text.Json.Serialization;

namespace QualstarLibrary
{
    public record LogEntry
    {
        public string Text { get; init; }
        public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.Now;
    }

    public class LibraryOperation
    {
        public bool Succeeded => Status.IsSuccess();
        public bool IsCompleted => Status != LibraryOperationStatus.Ongoing;
        public LibraryOperationStatus Status { get; private set; }

        public string? Message { get; private set; }

        private object _lock = new object();
        private List<LogEntry> _logs = new List<LogEntry>();
        private DateTimeOffset? _logTimestamp = null;
        public IReadOnlyCollection<LogEntry> Logs
        {
            get
            {
                lock (_lock)
                {
                    return _logs.Where(l => _logTimestamp == null || l.Timestamp > _logTimestamp).ToArray();
                }
            }
        }

        public DateTimeOffset StartedAt { get; } = DateTimeOffset.Now;
        private DateTimeOffset? _endedAt = null;
        public TimeSpan Elapsed => (_endedAt ?? DateTimeOffset.Now) - StartedAt;

        public TimeSpan? WaitBeforeNextOperation { get; private set; }
        public TimeSpan? WaitBeforeNextTrace { get; private set; }
        public DateTimeOffset Timestamp { get; private set; } = DateTimeOffset.Now;

        public string TraceId { get; private set; } = Guid.NewGuid().ToString();
        public LibraryOperation SetTraceId(string traceId)
        {
            TraceId = traceId;
            return this;
        }
        public LibraryOperation AddLog(string log)
        {
            lock (_lock)
            {
                _logs.Add(new LogEntry { Text = log });
            }
            return this;
        }
        public LibraryOperation LogFrom(DateTimeOffset time)
        {
            lock (_lock)
            {
                _logTimestamp = time;
            }
            return this;
        }
        public LibraryOperation AllLogs()
        {
            lock (_lock)
            {
                _logTimestamp = null;
            }
            return this;
        }

        /// <summary>
        /// Wait before next trace
        /// </summary>
        /// <param name="timeSpan"></param>
        public void Wait(TimeSpan timeSpan)
        {
            Timestamp = DateTimeOffset.Now;
            WaitBeforeNextTrace = timeSpan;
        }

        /// <summary>
        /// Wait before next operation
        /// </summary>
        /// <param name="timeSpan"></param>
        /// <returns></returns>
        public LibraryOperation WaitBeforeNext(TimeSpan timeSpan)
        {
            Timestamp = DateTimeOffset.Now;
            WaitBeforeNextOperation = timeSpan;
            return this;
        }

        public LibraryOperation SetStatus(LibraryOperationStatus status, string? message = default)
        {
            lock (_lock)
            {
                Status = status;
                if (!string.IsNullOrEmpty(message))
                {
                    Message = message;
                }
                if (status != LibraryOperationStatus.Ongoing)
                {
                    _endedAt = DateTimeOffset.Now;
                    WaitBeforeNextTrace = null;
                }
            }
            return this;
        }
        [JsonConstructor]
        public LibraryOperation(LibraryOperationStatus status, string? message = null)
        {
            Status = status;
            Message = message;
            if (Message == null)
            {
                Message = typeof(LibraryOperationStatus).GetField(status.ToString())?
                    .GetCustomAttribute<DisplayAttribute>()?.Description;
            }
        }

        #region factories
        public static LibraryOperation Success => new(LibraryOperationStatus.Succeeded);
        public static LibraryOperation DriveNotFound => new(LibraryOperationStatus.DriveNotFound);
        public static LibraryOperation TapeNotFound => new(LibraryOperationStatus.TapeNotFound);
        public static LibraryOperation Fail(string message) => new(LibraryOperationStatus.Failed, message);

        public static LibraryOperation Fail(Exception ex) => new(LibraryOperationStatus.Failed, ex.InnerException?.Message ?? ex.Message);

        public static LibraryOperation Ongoing(TimeSpan? wait)
            => new LibraryOperation(LibraryOperationStatus.Ongoing) { WaitBeforeNextTrace = wait };
        public static LibraryOperation NotSupported => new(LibraryOperationStatus.NotSupported);
        public static LibraryOperation DriveBusy => new LibraryOperation(LibraryOperationStatus.DriveBusy).WaitBeforeNext(TimeSpan.FromSeconds(15));

        public static LibraryOperation MtxBusy => new LibraryOperation(LibraryOperationStatus.MtxBusy).WaitBeforeNext(TimeSpan.FromSeconds(15));
        #endregion

        public override string ToString()
        {
            return Message ??
                (Logs.Any()
                ? Logs.Last().Text
                : Status.ToString());
        }
    }
}

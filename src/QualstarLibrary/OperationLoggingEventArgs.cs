namespace QualstarLibrary
{
    public class OperationLoggingEventArgs
    {
        public string? OperationId { get; init; }
        public string Message { get; init; }
        public OperationLoggingEventArgs(string? operationId, string message)
        {
            Message = message;
            OperationId = operationId;
        }
    }
}

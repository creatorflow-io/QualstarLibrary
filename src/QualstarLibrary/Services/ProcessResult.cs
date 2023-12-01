namespace QualstarLibrary.Services
{
    internal class ProcessResult
    {
        public int ExitCode { get; init; }
        public string? Output { get; init; }
        public ProcessResult(int exitCode, string? output)
        {
            ExitCode = exitCode;
            Output = output;
        }
    }
}

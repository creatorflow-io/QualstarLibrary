namespace QualstarLibrary
{
    public interface IOperationRepository
    {
        public Task<LibraryOperation> GetOperationAsync(string operationId, CancellationToken token);
        public Task AddOperationAsync(LibraryOperation operation, CancellationToken token);
        public Task UpdateOrAddOperationAsync(LibraryOperation operation, CancellationToken token);
    }
}

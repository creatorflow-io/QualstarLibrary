namespace QualstarLibrary.Services.Linux
{
    internal record Filesystem
    {
        public Filesystem(string name, long size, long avail, string mountedOn)
        {
            Name = name;
            Size = size;
            Avail = avail;
            MountedOn = mountedOn;
        }

        public string Name { get; init; }
        public long Size { get; init; }
        public long Avail { get; init; }
        public string MountedOn { get; init; }
    }
}

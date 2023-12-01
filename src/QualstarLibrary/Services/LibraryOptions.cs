namespace QualstarLibrary.Services
{
    internal class LibraryOptions
    {
        public string? LtfsPath { get; set; }
        public string? MtxPath { get; set; }

        public MtxDrive[] Drives { get; set; } = new MtxDrive[0];
    }

    internal class MtxDrive
    {
        public uint SlotNumber { get; set; }
        public string Address { get; set; }
        public string? Serial { get; set; }
    }
}

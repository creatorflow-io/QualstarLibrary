namespace QualstarLibrary
{
    public class MediaEventArgs : EventArgs
    {
        public string VolumeTag { get; init; }
        public MediaEventArgs(string volumeTag)
        {
            VolumeTag = volumeTag;
        }
    }
}

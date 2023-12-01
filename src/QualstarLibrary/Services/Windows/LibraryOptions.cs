namespace QualstarLibrary.Services.Windows
{
    internal class LibraryOptions : Services.LibraryOptions
    {
        /// <summary>
        /// The only difference is in the naming of devices.
        /// <para>On Linux the changer is accessed using /dev/sg{N}, on Windows you use Changer{N}.</para>
        /// <para>On Linux the changer device is not permanent so we have to collect it by the command "ls /dev/sg -l"</para>
        /// </summary>
        public int? MtxChanger { get; set; }
    }
}

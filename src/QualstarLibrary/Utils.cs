namespace QualstarLibrary
{
    public class Utils
    {
        public long L5 { get; set; } = 150000000000L;
        public long L6 { get; set; } = 250000000000L;
        public long L7 { get; set; } = 600000000000L;
        public long L8 { get; set; } = 1200000000000L;
        public long L9 { get; set; } = 1800000000000L;

        public static long NativeCapacity(string genShortName)
        {
            return (long?)typeof(Utils).GetProperty(genShortName)?.GetValue(null, null) ?? 0;
        }

        public static string GenName(string genShortName)
        {
            return genShortName switch
            {
                "L5" => "LTO-5",
                "L6" => "LTO-6",
                "L7" => "LTO-7",
                "L8" => "LTO-8",
                "L9" => "LTO-9",
                _ => throw new Exception($"Unknown LTO generation: {genShortName}")
            };
        }

        public static string GenShortName(string volumeTag)
        {
            if (volumeTag.Length != 8)
            {
                throw new Exception($"Invalid volume tag: {volumeTag}");
            }
            return volumeTag.Substring(volumeTag.Length - 2, 2);
        }

        public static string TapeSerial(string volumeTag)
        {
            if (volumeTag.Length != 8)
            {
                throw new Exception($"Invalid volume tag: {volumeTag}");
            }
            return volumeTag.Substring(0, 6);
        }
    }
}

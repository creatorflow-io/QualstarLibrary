using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
namespace QualstarLibrary
{
    public static class LibraryServiceCollectionExtensions
    {
        public static IServiceCollection AddWindowsQualStarLibrary(this IServiceCollection services, IConfigurationSection configuration)
        {
            services.Configure<Services.Windows.LibraryOptions>(configuration);
            var options = configuration.Get<Services.Windows.LibraryOptions>();
            if (!options.MtxChanger.HasValue)
            {
                throw new Exception("MtxChanger is required");
            }
            services.AddSingleton<Services.Windows.Library>();
            services.AddSingleton<ILibrary>(sp => sp.GetRequiredService<Services.Windows.Library>());
            return services;
        }


        public static IServiceCollection AddLinuxQualStarLibrary(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<Services.Linux.LibraryOptions>(configuration);
            var options = configuration.Get<Services.Linux.LibraryOptions>();
            if (string.IsNullOrEmpty(options.MountPoint))
            {
                throw new Exception("MountPoint is required");
            }
            services.AddSingleton<Services.Linux.Library>();
            services.AddSingleton<ILibrary>(sp => sp.GetRequiredService<Services.Linux.Library>());
            return services;
        }

    }
}

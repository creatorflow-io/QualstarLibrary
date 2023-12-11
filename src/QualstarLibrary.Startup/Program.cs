using System.Diagnostics;
using Juice.Extensions.Logging;
using Juice.Locks.InMemory;
using QualstarLibrary;

WebApplicationOptions options = new()
{
    ContentRootPath = AppContext.BaseDirectory,
    Args = args.Where(arg => arg != "--console").ToArray()
};

var builder = WebApplication.CreateBuilder(options);

ConfigureServices(builder);

var app = builder.Build();

ConfigureApp(app);
app.Run();

void ConfigureServices(WebApplicationBuilder builder)
{
    var platform = Environment.OSVersion.Platform;
    var isService = !(Debugger.IsAttached || args.Contains("--console"));

    if (platform == PlatformID.Win32NT)
    {
        builder.Services.AddWindowsQualStarLibrary(builder.Configuration.GetSection("TapeLibrary"));
        if (isService)
        {
            builder.Host.UseWindowsService();
        }
    }
    else if (platform == PlatformID.Unix)
    {
        builder.Services.AddLinuxQualStarLibrary(builder.Configuration.GetSection("TapeLibrary"));
        if (isService)
        {
            builder.Host.UseSystemd();
        }
    }
    else
    {
        throw new NotSupportedException("Unsupported platform");
    }

    builder.Services.AddInMemoryLock();

    builder.Logging.AddFileLogger(builder.Configuration.GetSection("Logging:File"));
}

async void ConfigureApp(WebApplication app)
{
    var library = app.Services.GetRequiredService<ILibrary>();

    library.DriveChanged += (s, e) =>
    {
        app.Logger.LogInformation("Drive changed: {Drive} {Op}", e.SlotNumber, e.Operation);
    };

    library.MediaChanged += (s, e) =>
    {
        app.Logger.LogInformation("Media changed: {Drive}", e.VolumeTag);
    };

    app.MapGet("/", () => "Welcome to the Library!\nTry /library/help to see how it's work");
    app.UseMiddleware<LibraryMiddleware>();
}

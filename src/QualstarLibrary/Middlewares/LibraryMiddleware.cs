using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace QualstarLibrary
{
    public class LibraryMiddleware
    {
        private readonly RequestDelegate _next;

        public LibraryMiddleware(RequestDelegate next)
        {
            _next = next;
        }

        public async Task InvokeAsync(HttpContext context)
        {
            if (!context.Request.Path.StartsWithSegments("/library"))
            {
                await _next(context);
                return;
            }

            var library = context.RequestServices.GetRequiredService<ILibrary>();

            var action = GetNextSegment(context.Request.Path, "/library");

            var jsonOptions = new JsonSerializerOptions
            {
                Converters =
                {
                    new JsonStringEnumConverter()
                },
                WriteIndented = true
            };

            switch (action)
            {
                case "drives":
                    {
                        var force = GetNextSegment(context.Request.Path, "/library/drives").Equals("force", StringComparison.OrdinalIgnoreCase);
                        await library.CollectStatusAsync(force, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(library.Drives, jsonOptions);
                        break;
                    }
                case "tapes":
                    {
                        var force = GetNextSegment(context.Request.Path, "/library/tapes").Equals("force", StringComparison.OrdinalIgnoreCase);
                        await library.CollectStatusAsync(force, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(await library.GetMediasAsync(context.RequestAborted), jsonOptions);
                        break;
                    }
                case "slots":
                    {
                        var force = GetNextSegment(context.Request.Path, "/library/slots").Equals("force", StringComparison.OrdinalIgnoreCase);
                        await library.CollectStatusAsync(force, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(library.Slots, jsonOptions);
                        break;
                    }
                case "data":
                    {
                        var force = GetNextSegment(context.Request.Path, "/library/data").Equals("force", StringComparison.OrdinalIgnoreCase);
                        await library.CollectStatusAsync(force, context.RequestAborted);
                        var data = new
                        {
                            Drives = library.Drives.ToArray(),
                            Slots = library.Slots.ToArray()
                        };

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(data, jsonOptions);
                        break;
                    }
                case "verify":
                    {
                        var result = await library.IsReadyAsync(context.RequestAborted);
                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "format":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var driveCode = GetNextSegment(context.Request.Path, "/library/format");
                        if (string.IsNullOrEmpty(driveCode) || !uint.TryParse(driveCode, out var slotNumber))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/format/{slotNumber}" });
                            return;
                        }
                        var force = GetNextSegment(context.Request.Path, "/library/format/" + driveCode).Equals("force", StringComparison.OrdinalIgnoreCase);
                        var result = await library.FormatAsync(slotNumber, force, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "ltfsck":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var driveCode = GetNextSegment(context.Request.Path, "/library/ltfsck");
                        if (string.IsNullOrEmpty(driveCode) || !uint.TryParse(driveCode, out var slotNumber))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/ltfsck/{slotNumber}" });
                            return;
                        }
                        var result = await library.LtfsckAsync(slotNumber, context.RequestAborted);
                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "load":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var driveCode = GetNextSegment(context.Request.Path, "/library/load");
                        if (string.IsNullOrEmpty(driveCode) || !uint.TryParse(driveCode, out var slotNumber))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/load/{slotNumber}/{tapeName}" });
                            return;
                        }
                        var tapeName = GetNextSegment(context.Request.Path, "/library/load/" + driveCode);
                        if (string.IsNullOrEmpty(tapeName))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/load/{driveCode}/{tapeName}" });
                            return;
                        }
                        var result = await library.LoadAsync(tapeName, slotNumber, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "unload":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var driveCode = GetNextSegment(context.Request.Path, "/library/unload");
                        if (string.IsNullOrEmpty(driveCode) || !uint.TryParse(driveCode, out var slotNumber))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/unload/{slotNumber}" });
                            return;
                        }
                        var result = await library.UnloadAsync(slotNumber, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "mount":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var driveCode = GetNextSegment(context.Request.Path, "/library/mount");
                        if (string.IsNullOrEmpty(driveCode) || !uint.TryParse(driveCode, out var slotNumber))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/mount/{slotNumber}" });
                            return;
                        }
                        var result = await library.MountAsync(slotNumber, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "unmount":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var driveCode = GetNextSegment(context.Request.Path, "/library/unmount");
                        if (string.IsNullOrEmpty(driveCode) || !uint.TryParse(driveCode, out var slotNumber))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/unmount/{slotNumber}" });
                            return;
                        }
                        var result = await library.UnmountAsync(slotNumber, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "transfer":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var tape = GetNextSegment(context.Request.Path, "/library/transfer");

                        if (string.IsNullOrEmpty(tape))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/transfer/{tapeName}/{slotNumber}" });
                            return;
                        }

                        var slot = GetNextSegment(context.Request.Path, "/library/transfer/" + tape);

                        if (!uint.TryParse(slot, out var slotNumber))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/transfer/{tapeName}/{slotNumber}" });
                            return;
                        }

                        var result = await library.TransferAsync(tape, slotNumber, context.RequestAborted);

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "release":
                    {
                        if (!context.Request.Method.Equals("POST", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                            await context.Response.WriteAsJsonAsync(new { error = "Invalid request" });
                            return;
                        }
                        var result = await library.ReleaseAsync(context.RequestAborted);
                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                        break;
                    }
                case "operation":
                    {
                        var traceId = GetNextSegment(context.Request.Path, "/library/operation");
                        if (string.IsNullOrEmpty(traceId))
                        {
                            context.Response.StatusCode = StatusCodes.Status400BadRequest;
                            await context.Response.WriteAsJsonAsync(new { error = "The request path must have format /library/operation/{traceId}" });
                            return;
                        }
                        var result = library.GetOperation(traceId);

                        if (result == null)
                        {
                            var repo = context.RequestServices.GetService<IOperationRepository>();
                            if (repo != null)
                            {
                                result = await repo.GetOperationAsync(traceId, context.RequestAborted);
                            }
                        }
                        var timestamp = GetNextSegment(context.Request.Path, "/library/operation/" + traceId);
                        if (!string.IsNullOrEmpty(timestamp) && long.TryParse(timestamp, out var ticks))
                        {
                            var time = new DateTimeOffset(ticks, DateTimeOffset.Now.Offset);
                            result?.LogFrom(time);
                        }

                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(result, jsonOptions);
                    }
                    break;
                case "help":
                    {
                        context.Response.StatusCode = StatusCodes.Status200OK;
                        await context.Response.WriteAsJsonAsync(new
                        {
                            operations = new
                            {
                                verify = new
                                {
                                    description = "Verify the library is ready",
                                    path = "/library/verify",
                                    method = "GET"
                                },
                                data = new
                                {
                                    description = "Get the current state of the library",
                                    path = "/library/data",
                                    method = "GET"
                                },
                                load = new
                                {
                                    description = "Load a tape into a drive then mount it",
                                    path = "/library/load/{slotNumber}/{tapeName}",
                                    method = "POST",
                                    parameters = new
                                    {
                                        slotNumber = "The slot number to load the tape into",
                                        tapeName = "The name of the tape to load"
                                    }
                                },
                                unload = new
                                {
                                    description = "Unmount then unload a tape from a drive",
                                    path = "/library/unload/{slotNumber}",
                                    method = "POST",
                                    parameters = new
                                    {
                                        slotNumber = "The slot number to unload the tape from"
                                    }
                                },
                                mount = new
                                {
                                    description = "Mount the tape in the drive by its slot number",
                                    path = "/library/mount/{slotNumber}",
                                    method = "POST",
                                    parameters = new
                                    {
                                        slotNumber = "The drive slot number to mount the tape"
                                    }
                                },
                                unmount = new
                                {
                                    description = "Unmount the tape in the drive by its slot number",
                                    path = "/library/unmount/{slotNumber}",
                                    method = "POST",
                                    parameters = new
                                    {
                                        slotNumber = "The drive slot number to unmount the tape"
                                    }
                                },
                                format = new
                                {
                                    description = "Format a tape in a drive",
                                    path = "/library/format/{slotNumber}",
                                    method = "POST",
                                    parameters = new
                                    {
                                        slotNumber = "The drive slot number to format the tape"
                                    }
                                },
                                ltfsck = new
                                {
                                    description = "Run ltfsck on a tape in a drive",
                                    path = "/library/ltfsck/{slotNumber}",
                                    method = "POST",
                                    parameters = new
                                    {
                                        slotNumber = "The drive slot number to run ltfsck on the tape"
                                    }
                                },
                                transfer = new
                                {
                                    description = "Transfer a tape from its slot to another slot",
                                    path = "/library/transfer/{tapeName}/{slotNumber}",
                                    method = "POST",
                                    parameters = new
                                    {
                                        tapeName = "The name of the tape to transfer",
                                        slotNumber = "The slot number to transfer the tape to"
                                    }
                                },
                                release = new
                                {
                                    description = "Release all drives and slots",
                                    path = "/library/release",
                                    method = "POST",
                                    parameters = new
                                    {
                                        none = "Releases all drives and slots"
                                    }
                                },
                                operation = new
                                {
                                    description = "Get the current state of an operation",
                                    path = "/library/operation/{traceId}",
                                    method = "GET",
                                    parameters = new
                                    {
                                        traceId = "The trace id of the operation"
                                    }
                                }
                            }
                        });
                        return;
                    }
                default:
                    await _next(context);
                    return;
            }

        }

        private string GetNextSegment(string path, string basePath)
        {
            var segments = path.Split('/');
            var baseSegments = basePath.Split('/');
            if (segments.Length <= baseSegments.Length)
            {
                return string.Empty;
            }
            return segments[baseSegments.Length];
        }
    }
}

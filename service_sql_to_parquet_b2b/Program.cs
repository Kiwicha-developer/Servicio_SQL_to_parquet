//Inicia el servicio usando la clase ParquetService
using service_sql_to_parquet_b2b;
using service_sql_to_parquet_b2b.Operations;
using Serilog;

class Program
{
    public static void Main(string[] args)
    {
        string logDirectory = Path.Combine(AppContext.BaseDirectory, "logs");
        //string logDirectory = @"D:\logs";
        

        if (!Directory.Exists(logDirectory))
        {
            Directory.CreateDirectory(logDirectory);
        }

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.File(
                path: Path.Combine(logDirectory, "service.log"),
                rollingInterval: RollingInterval.Day,
                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level}] {SourceContext} - {Message:lj}{NewLine}{Exception}"
            )
            .CreateLogger();
        try
        {
            Log.Information("Iniciando el servicio...");

            Host.CreateDefaultBuilder(args)
                .UseWindowsService()
                .UseSerilog()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddSingleton<DatabaseOperations>();
                    services.AddSingleton<FileOperations>();
                    services.AddHostedService<HostedService>();
                })
                .Build()
                .Run();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "El servicio terminó de forma inesperada");
        }
        finally
        {
            Log.CloseAndFlush(); 
        }
    }
}


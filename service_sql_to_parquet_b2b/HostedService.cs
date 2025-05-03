
using Apache.Arrow;
using service_sql_to_parquet_b2b.Operations;
using System.Data;

namespace service_sql_to_parquet_b2b
{
    public class HostedService : BackgroundService
    {
        private DatabaseOperations _databaseOperations;
        private FileOperations _fileOperations;
        private readonly ILogger<HostedService> _logger;
        private DateTime _lastExec = DateTime.MinValue;

        public HostedService(DatabaseOperations databaseOperations, FileOperations fileOperations, ILogger<HostedService> logger)
        {
            _databaseOperations = databaseOperations;
            _fileOperations = fileOperations;
            _logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Dictionary<string, string> config;

            try
            {
                string rutaBase = AppContext.BaseDirectory;
                config = ReadConfiguration(Path.Combine(rutaBase, "config.txt"));
                _logger.LogInformation("Configuración cargada correctamente.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al leer el archivo de configuración.");
                return;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                //Obtenemos Hora actual
                DateTime today = DateTime.Now;
                TimeSpan timer = today.TimeOfDay;
                TimeSpan exec = new TimeSpan(int.Parse(config["HOUR"]), int.Parse(config["MINUTE"]),00);
                if (timer.Hours == exec.Hours && timer.Minutes == exec.Minutes )
                {
                    // Evitar doble ejecución en el mismo minuto
                    if ((today - _lastExec).TotalMinutes >= 1)
                    {
                        await ExecuteOperations(); 
                        _lastExec = today;
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(60), stoppingToken);
            }
        }

        private async Task ExecuteOperations()
        {
            _logger.LogInformation("Inicializando proceso");

            Dictionary<string, string> config;

            try
            {
                string rutaBase = AppContext.BaseDirectory;
                config = ReadConfiguration(Path.Combine(rutaBase, "config.txt"));
                _logger.LogInformation("Configuración cargada correctamente.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al leer el archivo de configuración.");
                return;
            }

            string apiUrl = config["API"].Replace("0.0.0.0", "localhost");
            string stringConnection = _databaseOperations.GetDatabaseConnection(config["SERVER"], config["DATABASE"], config["USER"], config["PASS"]);
            int chunkSize = int.Parse(config["CHUNKSIZE"]);

            // ---- VENTAS ----
            try
            {
                IEnumerable<DataTable> chunksVentas = _databaseOperations.VentasQueryInChunks(stringConnection, config["PERIOD"], chunkSize);
                int indexVentas = 1;

                foreach (DataTable chunk in chunksVentas)
                {
                    try
                    {
                        string dirVentas = _fileOperations.SaveChunkToParquet(chunk, "venta", Path.Combine(config["TEMP"], "ventas"), indexVentas);
                        HttpResponseMessage response = await _fileOperations.UploadChunkToApi(dirVentas, apiUrl, chunksVentas.Count(), indexVentas, config["PERIOD"], "ventas");
                        string responseBody = await response.Content.ReadAsStringAsync();

                        _logger.LogInformation("Respuesta de la API (ventas), chunk {Index}: {Response}", indexVentas, responseBody);
                        indexVentas++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error procesando chunk {Index} de ventas", indexVentas);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en el procesamiento de ventas.");
            }

            // ---- INVENTARIO ----
            try
            {
                IEnumerable<DataTable> chunksInventory = _databaseOperations.InventarioQueryInChunks(stringConnection, config["PERIOD"], chunkSize);
                int indexInventory = 1;

                foreach (DataTable chunk in chunksInventory)
                {
                    try
                    {
                        string dirInventory = _fileOperations.SaveChunkToParquet(chunk, "inventario", Path.Combine(config["TEMP"], "inventario"), indexInventory);
                        HttpResponseMessage response = await _fileOperations.UploadChunkToApi(dirInventory, apiUrl, chunksInventory.Count(), indexInventory, config["PERIOD"], "inventario");
                        string responseBody = await response.Content.ReadAsStringAsync();

                        _logger.LogInformation("Respuesta de la API (inventario), chunk {Index}: {Response}", indexInventory, responseBody);
                        indexInventory++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error procesando chunk {Index} de inventario", indexInventory);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en el procesamiento de inventario.");
            }

            _logger.LogInformation("Proceso finalizado.");
        }

        private Dictionary<string, string> ReadConfiguration(string rutaArchivo)
        {
            var configuracion = new Dictionary<string, string>();

            foreach (string linea in File.ReadAllLines(rutaArchivo))
            {
                if (string.IsNullOrWhiteSpace(linea) || linea.StartsWith("#")) continue; // permite comentarios

                var partes = linea.Split('=', 2);
                if (partes.Length == 2)
                {
                    string clave = partes[0].Trim();
                    string valor = partes[1].Trim();
                    configuracion[clave] = valor;
                }
            }

            return configuracion;
        }
    }
}

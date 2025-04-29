using Apache.Arrow;
using ParquetSharp;
using System.Data;
using System.Linq;
using System.Net.Http.Headers;

namespace service_sql_to_parquet_b2b.Operations
{
    public class FileOperations
    {
        private readonly ILogger<FileOperations> _logger;

        public FileOperations(ILogger<FileOperations> logger)
        {
            _logger = logger;
        }
        public string SaveChunkToParquet(DataTable table, string type, string outputPath, int chunkNumber)
        {
            try
            {
                string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                string filename = $"{type}_data_{timestamp}_chunk_{chunkNumber}.parquet";
                string fullPath = Path.Combine(outputPath, filename);

                if (!Directory.Exists(outputPath))
                {
                    Directory.CreateDirectory(outputPath);
                    _logger.LogInformation("Directorio creado: {OutputPath}", outputPath);
                }

                var columns = table.Columns.Cast<DataColumn>()
                    .Select(col => new ParquetSharp.Column(col.DataType, col.ColumnName))
                    .ToArray();

                using (var fileWriter = new ParquetFileWriter(fullPath, columns))
                {
                    using (RowGroupWriter rg = fileWriter.AppendRowGroup())
                    {
                        foreach (DataColumn column in table.Columns)
                        {
                            System.Array data = table.Rows.Cast<DataRow>()
                                .Select(row => row[column])
                                .ToArray();

                            WriteColumn(rg, column.DataType, data);
                        }
                    }

                    fileWriter.Close();
                }

                _logger.LogInformation("Archivo Parquet guardado exitosamente: {Path}", fullPath);
                return fullPath;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al guardar el chunk {ChunkNumber} en Parquet en {OutputPath}", chunkNumber, outputPath);
                throw;
            }
        }

        public async Task<HttpResponseMessage> UploadChunkToApi(string filePath, string apiUrl, int totalChunks, int chunkNumber, string period, string type)
        {
            try
            {
                using (var client = new HttpClient())
                using (var form = new MultipartFormDataContent())
                using (var fileStream = File.OpenRead(filePath))
                {
                    var fileContent = new StreamContent(fileStream);
                    fileContent.Headers.ContentType = MediaTypeHeaderValue.Parse("application/octet-stream");
                    form.Add(fileContent, "file", Path.GetFileName(filePath));

                    form.Add(new StringContent(totalChunks.ToString()), "total_chunks");
                    form.Add(new StringContent(chunkNumber.ToString()), "chunk_number");
                    form.Add(new StringContent(period), "periodo");
                    form.Add(new StringContent(type), "tipo");

                    var response = await client.PostAsync(apiUrl, form);

                    if (!response.IsSuccessStatusCode)
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        _logger.LogWarning("Respuesta HTTP no exitosa: {Status} - {Reason}. Detalle: {Content}",
                            response.StatusCode, response.ReasonPhrase, errorContent);
                    }

                    return response;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error al subir el chunk {chunkNumber} de {totalChunks} del archivo {filePath} al endpoint {apiUrl}");
                throw;
            }
        }

        private void WriteColumn(RowGroupWriter rg, Type columnType, System.Array data)
        {
            var underlyingType = Nullable.GetUnderlyingType(columnType) ?? columnType;

            if (underlyingType == typeof(string))
                rg.NextColumn().LogicalWriter<string>().WriteBatch(data.Cast<string>().ToArray());
            else if (underlyingType == typeof(int))
            {
                if (Nullable.GetUnderlyingType(columnType) != null)
                    rg.NextColumn().LogicalWriter<int?>().WriteBatch(data.Cast<int?>().ToArray());
                else
                    rg.NextColumn().LogicalWriter<int>().WriteBatch(data.Cast<int>().ToArray());
            }
            else if (underlyingType == typeof(long))
            {
                if (Nullable.GetUnderlyingType(columnType) != null)
                    rg.NextColumn().LogicalWriter<long?>().WriteBatch(data.Cast<long?>().ToArray());
                else
                    rg.NextColumn().LogicalWriter<long>().WriteBatch(data.Cast<long>().ToArray());
            }
            else if (underlyingType == typeof(double))
            {
                if (Nullable.GetUnderlyingType(columnType) != null)
                    rg.NextColumn().LogicalWriter<double?>().WriteBatch(data.Cast<double?>().ToArray());
                else
                    rg.NextColumn().LogicalWriter<double>().WriteBatch(data.Cast<double>().ToArray());
            }
            else if (underlyingType == typeof(decimal))
            {
                if (Nullable.GetUnderlyingType(columnType) != null)
                    rg.NextColumn().LogicalWriter<decimal?>().WriteBatch(data.Cast<decimal?>().ToArray());
                else
                    rg.NextColumn().LogicalWriter<decimal>().WriteBatch(data.Cast<decimal>().ToArray());
            }
            else if (underlyingType == typeof(DateTime))
            {
                if (Nullable.GetUnderlyingType(columnType) != null)
                    rg.NextColumn().LogicalWriter<DateTime?>().WriteBatch(data.Cast<DateTime?>().ToArray());
                else
                    rg.NextColumn().LogicalWriter<DateTime>().WriteBatch(data.Cast<DateTime>().ToArray());
            }
            else
            {
                throw new NotSupportedException($"Tipo no soportado: {columnType}");
            }
        }

    }
}

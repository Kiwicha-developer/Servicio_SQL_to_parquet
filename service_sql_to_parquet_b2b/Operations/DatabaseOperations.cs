using Microsoft.Data.SqlClient;
using System.Data;

namespace service_sql_to_parquet_b2b.Operations
{
    public class DatabaseOperations
    {
        private readonly ILogger<DatabaseOperations> _logger;

        public DatabaseOperations(ILogger<DatabaseOperations> logger)
        {
            _logger = logger;
        }
        public string GetDatabaseConnection(string server,string database,string user,string pass)
        {
            return $"Server={server};Database={database};User Id={user};Password={pass};Encrypt=True;TrustServerCertificate=True;";
        }

        public IEnumerable<DataTable> ExecuteQueryInChunks(string connectionString, string query, string period, int chunkSize)
        {
            int offset = 0;
            string newPeriod = period == "auto" ? DateTime.Now.ToString("yyyyMM") : period;

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                try
                {
                    conn.Open();
                    _logger.LogInformation("Conexión abierta exitosamente a la base de datos.");
                }
                catch (SqlException sqlEx)
                {
                    _logger.LogError(sqlEx, "No se pudo abrir la conexión a la base de datos.");
                    throw new Exception("No se pudo abrir la conexión con la base de datos. Verifica tu cadena de conexión.", sqlEx);
                }

                while (true)
                {
                    DataTable chunk = new DataTable();

                    try
                    {
                        using (SqlCommand cmd = new SqlCommand(query, conn))
                        {
                            cmd.CommandTimeout = 1800; // 30 minutos
                            cmd.Parameters.AddWithValue("@period", newPeriod);
                            cmd.Parameters.AddWithValue("@Offset", offset);
                            cmd.Parameters.AddWithValue("@NextOffset", offset + chunkSize);

                            using (SqlDataReader reader = cmd.ExecuteReader())
                            {
                                chunk.Load(reader);
                                _logger.LogInformation("Chunk de datos leído: Offset {Offset} - Filas: {RowCount}", offset, chunk.Rows.Count);
                            }
                        }
                    }
                    catch (SqlException sqlEx)
                    {
                        _logger.LogError(sqlEx, "Error al ejecutar la consulta SQL con offset {Offset}", offset);
                        throw new Exception("Error al ejecutar la consulta SQL. Revisa la sintaxis o los datos enviados.", sqlEx);
                    }

                    if (chunk.Rows.Count == 0)
                    {
                        _logger.LogInformation("No se encontraron más datos para el período {Period}. Finalizando paginación.", newPeriod);
                        yield break;
                    }

                    yield return chunk;
                    offset += chunkSize;
                }
            }
        }

        public IEnumerable<DataTable> InventarioQueryInChunks(string connectionString, string period, int chunkSize)
        {
            string query = @"
                            WITH CV_LK_PRODUCTO_PROVEEDOR_2 AS (
                                SELECT 
                                    ID_PRODUCTO_PROVEEDOR,
                                    ISNULL(PKID_PRODUCTO_PROVEEDOR, 0) AS PKID_PRODUCTO_PROVEEDOR,
                                    ISNULL(REPLACE(REPLACE(C.DESC_PRODUCTO_PROVEEDOR, CHAR(39), ''), CHAR(10), ''), 'Sin categoria') AS DESC_PRODUCTO_PROVEEDOR,
                                    ISNULL(COD_PRODUCTO_PROVEEDOR, 0) AS COD_PRODUCTO_PROVEEDOR,
                                    A.PKID AS PKID_AGRUPACION
                                FROM CV_LK_PRODUCTO_PROVEEDOR C
                                INNER JOIN AGRUPACION_PROVEEDOR A ON A.DESC_PROVEEDOR = C.DESC_PRODUCTO_PROVEEDOR
                            ),
                            BaseData AS (
                                SELECT 
                                    cast(left(i.ID_DIA,6) as varchar(6))  AS Periodo,
                                    d.fecha AS FECHA,
                                    ag.PKID AS ID_PROVEEDOR,
                                    ag.DESC_PROVEEDOR AS DESC_PROVEEDOR,
                                    m.DESC_PRODUCTO_MARCA AS Marca,
                                    l.DESC_PRODUCTO_LINEA AS Linea,
                                    a.DESC_ALMACEN AS Almacen,
                                    s.DESC_SUCURSAL AS Sucursal,
                                    ca.DESC_PRODUCTO_CATEGORIA AS Categoria,
                                    p.Cod_Producto AS CodProducto,
                                    p.Desc_Producto AS Producto,
                                    me.DESC_MES AS Mes,
                                    an.DESC_ANIO AS Anio,
                                    mu.DESC_PRODUCTO_MUNDO AS Mundo,
                                    fs.DESC_FORMATO_SUCURSAL AS Formato,
                                    s.COD_SUCURSAL AS COD_SUCURSAL,
                                    COALESCE(MA.CodigoBarras, '0') AS CODIGOBARRAS,
                                    MAX(i.FactorMax) AS FactorMax,
                                    ROUND(SUM(i.StockFisico), 2) AS StockFisico,
                                    ROUND(SUM(i.TCP), 2) AS TCP,
                                    ROUND(SUM(i.Cuc), 2) AS CUC,
                                    ROUND(SUM(i.CPC), 2) AS CPC,
                                    ROUND(SUM(i.TUC), 2) AS TUC,
                                    ROUND(SUM(i.Cantidad), 2) AS Cantidad,
                                    ROUND(SUM(COALESCE(CAST(i.Cantidad AS DECIMAL) / NULLIF(i.FactorMax, 0), 0)), 2) AS Cajas,
                                    ROW_NUMBER() OVER (ORDER BY LEFT(CAST(i.ID_DIA AS VARCHAR(6)), 6)) AS RowNum
                                FROM CV_FT_INVENTARIO_DIA i
                                INNER JOIN CV_LK_DIA d ON i.ID_DIA = d.ID_DIA
                                INNER JOIN CV_LK_PRODUCTO p ON p.Id_Producto = i.ID_PRODUCTO
                                INNER JOIN CV_LK_PRODUCTO_PROVEEDOR_2 pp ON pp.ID_PRODUCTO_PROVEEDOR = p.Id_Producto_Proveedor
                                INNER JOIN CV_LK_PRODUCTO_MARCA m ON m.ID_PRODUCTO_MARCA = p.Id_Producto_Marca
                                INNER JOIN CV_LK_PRODUCTO_LINEA l ON l.ID_PRODUCTO_LINEA = p.Id_Producto_Linea
                                INNER JOIN CV_LK_ALMACEN a ON a.ID_ALMACEN = i.ID_ALMACEN
                                INNER JOIN CV_LK_SUCURSAL s ON s.ID_SUCURSAL = i.ID_SUCURSAL
                                INNER JOIN CV_LK_PRODUCTO_CATEGORIA ca ON ca.ID_PRODUCTO_CATEGORIA = l.ID_PRODUCTO_CATEGORIA
                                INNER JOIN CV_LK_MES me ON me.ID_MES = d.ID_MES
                                INNER JOIN CV_LK_ANIO an ON an.ID_ANIO = me.ID_ANIO
                                INNER JOIN CV_LK_PRODUCTO_MUNDO mu ON mu.ID_PRODUCTO_MUNDO = ca.ID_PRODUCTO_MUNDO
                                INNER JOIN AGRUPACION_PROVEEDOR ag ON ag.PKID = pp.PKID_AGRUPACION
                                INNER JOIN CV_LK_FORMATO_SUCURSAL fs ON fs.ID_FORMATO_SUCURSAL = s.ID_FORMATO_SUCURSAL
                                LEFT JOIN PSCV_MaestroProducto MA ON MA.ProductoCodigo = p.Cod_Producto
                                WHERE LEFT(i.ID_DIA,6) = @period
                                GROUP BY 
                                    i.ID_DIA,
                                    d.fecha,
                                    ag.PKID,
                                    ag.DESC_PROVEEDOR,
                                    m.DESC_PRODUCTO_MARCA,
                                    l.DESC_PRODUCTO_LINEA,
                                    a.DESC_ALMACEN,
                                    s.DESC_SUCURSAL,
                                    ca.DESC_PRODUCTO_CATEGORIA,
                                    p.Cod_Producto,
                                    p.Desc_Producto,
                                    me.DESC_MES,
                                    an.DESC_ANIO,
                                    mu.DESC_PRODUCTO_MUNDO,
                                    fs.DESC_FORMATO_SUCURSAL,
                                    s.COD_SUCURSAL,
                                    MA.CodigoBarras
                            )
                            SELECT * FROM BaseData
                            WHERE RowNum > @Offset
                            AND RowNum <= @NextOffset
                            ORDER BY Periodo;
                            ";

            //string query = @"
            //    WITH BaseData AS (
            //        SELECT 
            //            *,
            //            ROW_NUMBER() OVER (ORDER BY CodigoBarras) AS RowNum
            //        FROM PSCV_MaestroProducto
            //    )
            //    SELECT *
            //    FROM BaseData
            //    WHERE RowNum > @Offset
            //    AND RowNum <= @NextOffset
            //    ORDER BY CodigoBarras;";

            return ExecuteQueryInChunks(connectionString,query,period,chunkSize);
        }

        public IEnumerable<DataTable> VentasQueryInChunks(string connectionString, string period, int chunkSize)
        {
            string query = @"
                            WITH tablacodigo AS (
                                SELECT ProductoCodigo, 
                                       ProductoDescripcion, 
                                       EstadoActivo, 
                                       MAX(CodigoBarras) AS CodigoBarras, 
                                       StatusCode, 
                                       MAX(SKU) AS SKU, 
                                       FactorMaxima, 
                                       MAX(CostoPromedio) AS CostoPromedio 
                                FROM PSCV_MaestroProducto
                                GROUP BY ProductoCodigo, ProductoDescripcion, EstadoActivo, StatusCode, FactorMaxima
                            ),  
                            CV_LK_PRODUCTO_PROVEEDOR_2 AS (
                                SELECT 
                                    ID_PRODUCTO_PROVEEDOR,
                                    ISNULL(PKID_PRODUCTO_PROVEEDOR, 0) AS PKID_PRODUCTO_PROVEEDOR,
                                    ISNULL(REPLACE(REPLACE(C.DESC_PRODUCTO_PROVEEDOR, CHAR(39), ''), CHAR(10), ''), 'Sin categoria') AS DESC_PRODUCTO_PROVEEDOR,
                                    ISNULL(COD_PRODUCTO_PROVEEDOR, 0) AS COD_PRODUCTO_PROVEEDOR,
                                    A.PKID AS PKID_AGRUPACION
                                FROM CV_LK_PRODUCTO_PROVEEDOR C
                                INNER JOIN AGRUPACION_PROVEEDOR A ON A.DESC_PROVEEDOR = C.DESC_PRODUCTO_PROVEEDOR
                            ),
                            BaseData AS (
                            SELECT  
                                   CAST(LEFT(v.ID_DIA, 6) AS VARCHAR(6)) AS Periodo,
                                   CONVERT(DATE, dd.fecha, 103) AS FECHA,  -- Para convertir de 'dd/mm/yyyy' a DATE
                                   me.DESC_MES AS MES,
                                   aa.DESC_ANIO AS ANIO,
                                   ca.DESC_PRODUCTO_CATEGORIA AS CATEGORIA,
                                   pp.Cod_Producto AS CodProducto,
                                   pp.Desc_Producto AS PRODUCTO,
                                   AG.PKID AS ID_PROVEEDOR,
                                   AG.DESC_PROVEEDOR AS DESC_PROVEEDOR,
                                   mm.DESC_PRODUCTO_MARCA AS DESC_MARCA,
                                   ll.DESC_PRODUCTO_LINEA AS DESC_LINEA,
                                   s.DESC_SUCURSAL AS Sucursal,
                                   FS.DESC_FORMATO_SUCURSAL AS Formato,
                                   s.COD_SUCURSAL AS COD_SUCURSAL,
                                   COALESCE(MP.CodigoBarras, '0') AS CODIGOBARRAS,
                                   SUM(ROUND(v.CANTIDAD, 8)) AS Cantidad,
                                   ROUND(SUM(COALESCE(v.cajas / NULLIF(v.MAXFACTOR, 0), 0)), 2) AS Cajas,  -- Divisiones fuera de SUM y COALESCE para evitar valores nulos
                                   ROUND(SUM(ROUND(v.VALORVENTA, 8)), 8) AS ValorVenta,
                                   ROW_NUMBER() OVER (ORDER BY LEFT(CAST(v.ID_DIA AS VARCHAR(6)), 6)) AS RowNum
                            FROM CV_FT_CONSOLIDADO_VENTAS v
                            INNER JOIN CV_LK_PRODUCTO pp ON pp.Id_Producto = v.ID_PRODUCTO
                            INNER JOIN CV_LK_PRODUCTO_LINEA ll ON ll.ID_PRODUCTO_LINEA = pp.Id_Producto_Linea
                            INNER JOIN CV_LK_PRODUCTO_MARCA mm ON mm.ID_PRODUCTO_MARCA = pp.Id_Producto_Marca
                            INNER JOIN CV_LK_DIA dd ON dd.ID_DIA = v.ID_DIA
                            INNER JOIN CV_LK_PRODUCTO_PROVEEDOR_2 p ON p.ID_PRODUCTO_PROVEEDOR = pp.Id_Producto_Proveedor
                            INNER JOIN CV_LK_PRODUCTO_CATEGORIA ca ON ca.ID_PRODUCTO_CATEGORIA = ll.ID_PRODUCTO_CATEGORIA
                            INNER JOIN CV_LK_MES me ON me.ID_MES = dd.ID_MES
                            INNER JOIN CV_LK_ANIO aa ON aa.ID_ANIO = me.ID_ANIO
                            INNER JOIN AGRUPACION_PROVEEDOR AG ON AG.PKID = p.PKID_AGRUPACION
                            INNER JOIN CV_LK_SUCURSAL s ON s.ID_SUCURSAL = v.ID_SUCURSAL
                            INNER JOIN CV_LK_FORMATO_SUCURSAL FS ON FS.ID_FORMATO_SUCURSAL = s.ID_FORMATO_SUCURSAL
                            LEFT JOIN tablacodigo MP ON MP.ProductoCodigo = pp.Cod_Producto
                            --WHERE p.ID_PROVEEDOR = 252
                            WHERE LEFT(v.ID_DIA, 6) = @period
                            GROUP BY v.ID_DIA,
                                     FECHA, 
                                     me.DESC_MES,
                                     aa.DESC_ANIO,
                                     ca.DESC_PRODUCTO_CATEGORIA,
                                     pp.Cod_Producto,
                                     pp.Desc_Producto,
                                     AG.PKID,
                                     AG.DESC_PROVEEDOR,
                                     mm.DESC_PRODUCTO_MARCA,
                                     ll.DESC_PRODUCTO_LINEA,
                                     s.DESC_SUCURSAL,
                                     FS.DESC_FORMATO_SUCURSAL,
                                     s.COD_SUCURSAL,
                                     MP.CodigoBarras

                               )
                                SELECT * FROM BaseData
                                WHERE RowNum > @Offset
                                AND RowNum <= @NextOffset
                                ORDER BY Periodo;
                            ";

            //string query = @"
            //    WITH tableCodigo AS (
            //        SELECT 
            //            DESC_ANIO,
            //            ROW_NUMBER() OVER (ORDER BY DESC_ANIO) AS RowNum
            //        FROM CV_LK_ANIO
            //    )
            //    SELECT *
            //    FROM tableCodigo
            //    WHERE RowNum > @Offset
            //    AND RowNum <= @NextOffset
            //    ORDER BY DESC_ANIO;";

            return ExecuteQueryInChunks(connectionString, query, period, chunkSize);
        }
    }
}

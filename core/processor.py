# core/processor.py

import polars as pl
from polars.exceptions import ColumnNotFoundError
import xlsxwriter
from settings import config as cfg

def cargar_y_limpiar_base(ruta_archivo: str) -> pl.DataFrame | None:
    """
    Función que carga y limpia los datos base.
    """
    print("1. Iniciando carga y procesamiento...")
    try:
        # Carga el archivo de Excel permitiendo a Polars inferir tipos
        df_raw = pl.read_excel(source=ruta_archivo, sheet_name=cfg.NOMBRE_HOJA_POR_DEFECTO)
        
        # Se asegura de que solo las columnas necesarias están presentes
        df_raw = df_raw.select([col for col in cfg.COLUMNAS_ORIGINALES if col in df_raw.columns])

        # Limpieza y conversión de tipos en modo lazy para eficiencia
        df_limpio = (
            df_raw.lazy()
            .with_columns(
                pl.col([cfg.COL_FECHA_OBJECION, cfg.COL_FECHA_CONTESTACION]).cast(pl.Date, strict=False),
                pl.when(pl.col(cfg.COL_FECHA_RADICADO).cast(pl.Utf8, strict=False).str.contains(cfg.TEXTO_NULO_FECHA))
                  .then(None).otherwise(pl.col(cfg.COL_FECHA_RADICADO)).cast(pl.Date, strict=False).alias(cfg.COL_FECHA_RADICADO),
                pl.col(cfg.COLUMNAS_DOCN).cast(pl.Utf8).str.replace_all(r"\.", "").cast(pl.Int64, strict=False),
                pl.col(cfg.COL_CUENTA_COBRO).cast(pl.Utf8).str.replace_all(r"\.", "").cast(pl.Int64, strict=False).fill_null(0),
                pl.col(cfg.COL_VR_GLOSA).cast(pl.Utf8).str.replace_all(r",", ".").cast(pl.Float64, strict=False).fill_null(0),
            )
            .filter(pl.col(cfg.COL_ESTATUS).is_in(cfg.ESTATUS_VALIDOS))
            .with_columns(pl.concat_str([pl.col(cfg.COL_SERIE), pl.col(cfg.COL_N_FACTURA).cast(pl.Utf8)], separator="").alias(cfg.COL_FACTURA_CONCAT))
            .collect())
        print("2. Datos base limpios y listos para clasificación.")
        return df_limpio
    except ColumnNotFoundError as e:
        print(f"ERROR: La columna '{e.column_name}' no se encontró. Revisa 'config.py'."); return None
    except Exception as e:
        print(f"ERROR crítico durante el procesamiento: {e}"); return None

def obtener_facturas_puras(df_base: pl.DataFrame, condicion: pl.Expr) -> pl.DataFrame:
    """
    Devuelve los ítems de facturas donde TODOS sus ítems cumplen una condición.
    """
    df_con_evaluacion = df_base.with_columns(
        es_factura_pura=condicion.all().over(cfg.COLUMNAS_FACTURA)
    )
    return df_con_evaluacion.filter(pl.col("es_factura_pura")).drop("es_factura_pura")

def crear_tabla_resumen_detalle(df_items_categoria: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma un DataFrame de ítems de una categoría en una estructura Resumen/Detalle.
    """
    if df_items_categoria.is_empty():
        return pl.DataFrame()

    # --- 1. Crear el DataFrame de Resumen ---
    df_resumen = df_items_categoria.group_by(cfg.COLUMNAS_FACTURA).agg([
        ((pl.col(cfg.COL_CUENTA_COBRO) != 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_not_null())).sum().alias("Items_ConCC_ConFR"),
        ((pl.col(cfg.COL_CUENTA_COBRO) != 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_null())).sum().alias("Items_ConCC_SinFR"),
        ((pl.col(cfg.COL_CUENTA_COBRO) == 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_not_null())).sum().alias("Items_SinCC_ConFR"),
        ((pl.col(cfg.COL_CUENTA_COBRO) == 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_null())).sum().alias("Items_SinCC_SinFR"),
        pl.count().alias("Total_Items_Factura"),
        pl.col(cfg.COL_ENTIDAD).first().alias(cfg.COL_ENTIDAD),
        pl.col(cfg.COL_FECHA_OBJECION).first().alias(cfg.COL_FECHA_OBJECION),
        pl.col(cfg.COL_FECHA_CONTESTACION).first().alias(cfg.COL_FECHA_CONTESTACION),
        pl.col(cfg.COL_FECHA_RADICADO).first().alias(cfg.COL_FECHA_RADICADO),
        pl.col(cfg.COL_FACTURA_CONCAT).first().alias(cfg.COL_FACTURA_CONCAT),
        pl.sum(cfg.COL_VR_GLOSA).alias(cfg.COL_VR_GLOSA),
    ]).with_columns(
        pl.lit("Resumen Factura").alias("TipoFila")
    )
    
    # --- 2. Preparar el Detalle ---
    df_detalle = df_items_categoria.with_columns(pl.lit("Detalle Ítem").alias("TipoFila"))

    # --- CORRECCIÓN CLAVE ---
    # 3. Forzar esquemas idénticos antes de la concatenación
    
    # Define TODAS las columnas que deben estar en el resultado final.
    # Estas son las columnas del detalle, más las que solo existen en el resumen.
    columnas_resumen_exclusivas = [
        "Total_Items_Factura", "Items_ConCC_ConFR", "Items_ConCC_SinFR",
        "Items_SinCC_ConFR", "Items_SinCC_SinFR"
    ]
    # Todas las columnas que deben tener las dos tablas.
    esquema_final_cols = df_detalle.columns + columnas_resumen_exclusivas
    
    # Asegurar que el df_detalle tenga todas las columnas, llenando las faltantes con nulos.
    df_detalle_alineado = df_detalle.select(
        pl.all(),
        *[pl.lit(None).alias(c) for c in columnas_resumen_exclusivas]
    )
    
    # Asegurar que ambas tablas tengan las columnas en el MISMO ORDEN.
    df_resumen_alineado = df_resumen.select(esquema_final_cols)
    df_detalle_alineado = df_detalle_alineado.select(esquema_final_cols)

    # 4. Concatenar. Ahora que los esquemas son idénticos, 'how="vertical"' es seguro.
    df_final = pl.concat([df_resumen_alineado, df_detalle_alineado], how="vertical")
    
    # 5. Ordenar el resultado final
    columnas_de_orden = cfg.COLUMNAS_FACTURA + ["TipoFila"]
    df_final = df_final.sort(
        by=columnas_de_orden,
        descending=[False] * len(cfg.COLUMNAS_FACTURA) + [True]
    )
    
    # Renombrar las columnas al final del todo, no es necesario seleccionar.
    # La exportación se encargará de esto. Devolvemos el DF con nombres internos.
    return df_final

def exportar_reporte_final(dict_tablas: dict, ruta_salida: str):
    """
    Exporta un diccionario de DataFrames a un único archivo Excel con formato.
    
    Args:
        dict_tablas: Diccionario con {nombre_hoja: DataFrame}.
        ruta_salida: El path donde se guardará el archivo.
    """
    print(f"\n5. Exportando reporte final a: {ruta_salida}")
    try:
        # Creamos el workbook de Excel en la ruta de salida
        workbook = xlsxwriter.Workbook(ruta_salida)

        # Iteramos sobre cada tabla/hoja que queremos crear
        for nombre_hoja, df_original in dict_tablas.items():
            if df_original.is_empty():
                print(f"   - Hoja '{nombre_hoja}' está vacía, se omitirá.")
                continue

            # Renombramos las columnas del DataFrame antes de escribirlo
            df_a_exportar = df_original.rename(cfg.COLUMN_NAME_MAPPING_EXPORT)
            
            # Pasamos tanto el DataFrame como el workbook a write_excel
            df_a_exportar.write_excel(
                workbook=workbook,
                sheet_name=nombre_hoja,
                autofilter=True,
                table_style=None
            )
            
            # Obtenemos la hoja recién creada para aplicarle formatos
            worksheet = workbook.get_worksheet_by_name(nombre_hoja)
            if worksheet:
                # ---- Aplicación de Formatos Celda por Celda ----
                
                # Creamos los objetos de formato
                formato_moneda = workbook.add_format({'num_format': '$ #,##0'})
                formato_entero = workbook.add_format({'num_format': '0'})
                formato_fecha = workbook.add_format({'num_format': 'dd/mm/yy'})

                # Obtenemos los nombres finales de las columnas
                nombres_finales_columnas = df_a_exportar.columns
                
                # Iteramos por las columnas del DataFrame exportado
                for idx, col_name in enumerate(nombres_finales_columnas):
                    formato_a_aplicar = None
                    # Buscamos el nombre original de la columna en el mapeo inverso
                    if col_name == cfg.COLUMN_NAME_MAPPING_EXPORT.get(cfg.COL_VR_GLOSA):
                        formato_a_aplicar = formato_moneda
                    elif col_name in [cfg.COLUMN_NAME_MAPPING_EXPORT.get(c) for c in [cfg.COL_N_FACTURA, cfg.COL_GL_DOCN, cfg.COL_CUENTA_COBRO]]:
                        formato_a_aplicar = formato_entero
                    elif col_name in [cfg.COLUMN_NAME_MAPPING_EXPORT.get(c) for c in cfg.COLUMNAS_FECHA]:
                        formato_a_aplicar = formato_fecha
                    
                    if formato_a_aplicar:
                        # Aplicamos el formato a toda la columna, saltando la cabecera
                        worksheet.set_column(idx, idx, None, formato_a_aplicar)
            
            print(f"   - Hoja '{nombre_hoja}' exportada y formateada con éxito.")
        
        # Es crucial cerrar el workbook para que se guarde el archivo
        workbook.close()
        print("\n¡Proceso completado! Archivo final generado.")

    except Exception as e:
        print(f"ERROR al guardar el archivo Excel final: {e}")
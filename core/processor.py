# core/processor.py
import polars as pl
import xlsxwriter
from settings import config as cfg
import pandas as pd

def print_debug_info(step_name, df, columns_to_show):
    """Función auxiliar para imprimir el estado del DataFrame en un punto de depuración."""
    print(f"\n--- DEBUG: {step_name} ---")
    if all(col in df.columns for col in columns_to_show):
        print("Columnas relevantes y sus tipos:")
        for col in columns_to_show:
            print(f"  - {col}: {df.schema[col]}")
        print("Primeros 5 valores:")
        print(df.select(columns_to_show).head())
    else:
        print(f"  (Una o más columnas de {columns_to_show} no existen en este paso)")
    print("------------------------------------------")
    
def cargar_y_limpiar_base(ruta_archivo: str) -> pl.DataFrame | None:
    """
    Versión final y correcta. No procesa las fechas que ya son correctas
    y aplica el tratamiento especial únicamente a 'fecha_rep'.
    """
    print("1. Iniciando carga y procesamiento...")
    try:
        # 1. Carga inicial. Polars convertirá 'fecha_objecion' y 'fecha_contestacion'
        # a datetime automáticamente porque reconoce el formato.
        df_raw = pl.read_excel(source=ruta_archivo, sheet_name=cfg.NOMBRE_HOJA_POR_DEFECTO)
        
        lazy_df = df_raw.lazy().filter(pl.col(cfg.COL_ESTATUS).is_in(cfg.ESTATUS_VALIDOS))
        
        # --- PASO 1: DEJAR LAS FECHAS CORRECTAS EN PAZ ---
        # No se aplica ningún tratamiento a 'fecha_objecion' y 'fecha_contestacion'.
        # Ya fueron leídas correctamente.
        print("   - 'fecha_objecion' y 'fecha_contestacion' se asumen correctas desde la carga.")

        # --- PASO 2: APLICAR TRATAMIENTO ESPECIAL SOLO A 'fecha_rep' ---
        # Verificamos si es texto y la parseamos. Es la única que lo necesita.
        if df_raw.schema[cfg.COL_FECHA_RADICADO] == pl.String:
            print(f"   - Tratamiento especial para '{cfg.COL_FECHA_RADICADO}': Detectada como STRING. Parseando texto...")
            lazy_df = lazy_df.with_columns(
                pl.when(pl.col(cfg.COL_FECHA_RADICADO).str.contains(cfg.TEXTO_NULO_FECHA))
                .then(None)
                .otherwise(
                    pl.col(cfg.COL_FECHA_RADICADO).str.to_datetime(
                        format="%Y-%m-%d %H:%M:%S", # El formato que sabemos que funciona para esta columna
                        strict=False
                    )
                )
                .alias(cfg.COL_FECHA_RADICADO)
            )
        else:
            # Fallback por si en algún archivo 'fecha_rep' viniera como número
            print(f"   - Tratamiento para '{cfg.COL_FECHA_RADICADO}': Detectada como NUMÉRICA.")
            fecha_base_excel = pl.date(1899, 12, 30)
            lazy_df = lazy_df.with_columns(
                (fecha_base_excel + pl.duration(days=pl.col(cfg.COL_FECHA_RADICADO)))
                .alias(cfg.COL_FECHA_RADICADO)
            )

        # --- PASO 3: Transformaciones finales y estandarización ---
        df_limpio = (
            lazy_df.with_columns(
                # Otras conversiones numéricas y de texto
                pl.col(cfg.COLUMNAS_DOCN).cast(pl.Utf8).str.replace_all(r"\.", "").cast(pl.Int64, strict=False),
                pl.col(cfg.COL_VR_GLOSA).cast(pl.Utf8).str.replace_all(r",", ".").cast(pl.Float64, strict=False).fill_null(0),
                pl.col(cfg.COL_CUENTA_COBRO).cast(pl.Utf8).str.replace_all(r"\.", "").cast(pl.Int64, strict=False).fill_null(0),
                
                # UNIFICACIÓN FINAL: Quitar la hora a TODAS las columnas de fecha para que sean 'Date'
                pl.col(cfg.COLUMNAS_FECHA).dt.date()
                
            ).with_columns(
                pl.concat_str([pl.col(cfg.COL_SERIE), pl.col(cfg.COL_N_FACTURA).cast(pl.Utf8)], separator="").alias(cfg.COL_FACTURA_CONCAT)
            ).collect()
        )
        
        print("2. Datos base limpios y listos para clasificación.")
        return df_limpio
    
    except Exception as e:
        print(f"ERROR crítico durante la carga o limpieza: {e}")
        return None
    
def obtener_facturas_puras(df_base: pl.DataFrame, condicion: pl.Expr) -> pl.DataFrame:
    df_con_evaluacion = df_base.with_columns(es_factura_pura=condicion.all().over(cfg.COLUMNAS_FACTURA))
    return df_con_evaluacion.filter(pl.col("es_factura_pura")).drop("es_factura_pura")

def crear_tabla_resumen_detalle(df_items_categoria: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma un DataFrame de ítems de una categoría en una estructura Resumen/Detalle.
    Versión robusta con alineación de esquema para evitar errores de tipo en concat.
    """
    if df_items_categoria.is_empty():
        return pl.DataFrame()

    # --- 1. Crear el DataFrame de Resumen (tu código, sin cambios) ---
    df_resumen = df_items_categoria.group_by(cfg.COLUMNAS_FACTURA).agg([
        pl.count().alias("Total Items Factura"),
        ((pl.col(cfg.COL_CUENTA_COBRO) != 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_not_null())).sum().alias("Con CC y Con FR"),
        ((pl.col(cfg.COL_CUENTA_COBRO) != 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_null())).sum().alias("Con CC y Sin FR"),
        ((pl.col(cfg.COL_CUENTA_COBRO) == 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_not_null())).sum().alias("Sin CC y Con FR"),
        ((pl.col(cfg.COL_CUENTA_COBRO) == 0) & (pl.col(cfg.COL_FECHA_RADICADO).is_null())).sum().alias("Sin CC y Sin FR"),
        pl.col(cfg.COL_ENTIDAD).first(),
        pl.col(cfg.COL_FECHA_OBJECION).first(),
        pl.col(cfg.COL_FECHA_CONTESTACION).first(),
        pl.col(cfg.COL_FECHA_RADICADO).first(),
        pl.col(cfg.COL_FACTURA_CONCAT).first(),
        pl.sum(cfg.COL_VR_GLOSA),
    ]).with_columns(
        pl.lit("Resumen Factura").alias("Tipo de Fila")
    )

    # --- 2. Crear el DataFrame de Detalle (tu código, sin cambios) ---
    df_detalle = df_items_categoria.with_columns(
        pl.lit("Detalle Ítem").alias("Tipo de Fila")
    )

    # --- 3. Alinear los esquemas antes de concatenar ---

    # ÚNICO CAMBIO: Usa la nueva lista de la configuración
    columnas_finales_ordenadas = cfg.COLUMNAS_FINALES_ORDENADAS

    # (El resto de tu lógica de alineación sigue siendo la misma y es correcta)
    # Alinear columnas en RESUMEN
    for col_name in columnas_finales_ordenadas:
        if col_name not in df_resumen.columns:
            if col_name in df_detalle.columns: tipo = df_detalle.schema[col_name]
            else: tipo = pl.Utf8
            df_resumen = df_resumen.with_columns(pl.lit(None, dtype=pl.Null).cast(tipo).alias(col_name))
    df_resumen = df_resumen.select(columnas_finales_ordenadas)

    # Alinear columnas en DETALLE
    for col_name in columnas_finales_ordenadas:
        if col_name not in df_detalle.columns:
            if col_name in df_resumen.columns: tipo = df_resumen.schema[col_name]
            else: tipo = pl.Utf8
            df_detalle = df_detalle.with_columns(pl.lit(None, dtype=pl.Null).cast(tipo).alias(col_name))
    df_detalle = df_detalle.select(columnas_finales_ordenadas)

    # --- 4. Concatenar (tu código, sin cambios) ---
    df_final = pl.concat([df_resumen, df_detalle], how="vertical")

    # --- 5. Ordenar (tu código, sin cambios) ---
    columnas_de_orden = cfg.COLUMNAS_FACTURA + ["Tipo de Fila"]
    df_final = df_final.sort(
        by=columnas_de_orden,
        descending=[False] * len(cfg.COLUMNAS_FACTURA) + [True]
    )

    return df_final

def exportar_reporte_final(dict_tablas: dict, ruta_salida: str):
    """
    Exporta un diccionario de DataFrames a un único archivo Excel, aplicando
    ajustes de formato y tipo de dato justo antes de escribir.
    """
    print(f"\n5. Exportando reporte final a: {ruta_salida}")
    try:
        with pd.ExcelWriter(ruta_salida, engine='xlsxwriter') as writer:

            for nombre_hoja, df_original in dict_tablas.items():
                if df_original.is_empty():
                    print(f"   - Hoja '{nombre_hoja}' está vacía, se omitirá.")
                    continue

                # --- AJUSTES FINALES JUSTO ANTES DE EXPORTAR ---
                df_ajustado = df_original.with_columns(
                    pl.when(pl.col(cfg.COL_CUENTA_COBRO) == 0)
                      .then(None)
                      .otherwise(pl.col(cfg.COL_CUENTA_COBRO))
                      .alias(cfg.COL_CUENTA_COBRO)
                )

                # Renombrar columnas
                df_a_exportar = df_ajustado.rename(cfg.COLUMN_NAME_MAPPING_EXPORT)
                df_pandas = df_a_exportar.to_pandas()

                # Convertir columnas de fecha a datetime.date (sin hora)
                columnas_fecha_renombradas = []
                for col in cfg.COLUMNAS_FECHA:
                    col_renombrado = cfg.COLUMN_NAME_MAPPING_EXPORT.get(col, col)
                    if col_renombrado in df_pandas.columns:
                        df_pandas[col_renombrado] = pd.to_datetime(df_pandas[col_renombrado], errors='coerce').dt.date
                        columnas_fecha_renombradas.append(col_renombrado)

                # Crear hoja manualmente
                worksheet = writer.book.add_worksheet(nombre_hoja)
                writer.sheets[nombre_hoja] = worksheet

                # Crear formatos
                formato_centrado = writer.book.add_format({'align': 'center', 'valign': 'vcenter'})
                formato_moneda = writer.book.add_format({'num_format': '$ #,##0', 'align': 'center', 'valign': 'vcenter'})
                formato_entero = writer.book.add_format({'num_format': '0', 'align': 'center', 'valign': 'vcenter'})
                formato_fecha = writer.book.add_format({'num_format': 'dd/mm/yy', 'align': 'center', 'valign': 'vcenter'})
                formato_encabezado = writer.book.add_format({'align': 'center', 'valign': 'vcenter', 'bold': True})

                # Escribir encabezados en negrilla
                for col_idx, col_name in enumerate(df_pandas.columns):
                    worksheet.write(0, col_idx, col_name, formato_encabezado)

                # Escribir datos fila por fila
                for row_idx, row in df_pandas.iterrows():
                    for col_idx, col_name in enumerate(df_pandas.columns):
                        valor = row[col_name]

                        # Elegir el formato base
                        if col_name in columnas_fecha_renombradas:
                            formato_base = formato_fecha
                        elif col_name == cfg.COLUMN_NAME_MAPPING_EXPORT.get(cfg.COL_VR_GLOSA):
                            formato_base = formato_moneda
                        elif col_name in [cfg.COLUMN_NAME_MAPPING_EXPORT.get(c) for c in [cfg.COL_N_FACTURA, cfg.COL_GL_DOCN, cfg.COL_CUENTA_COBRO]]:
                            formato_base = formato_entero
                        else:
                            formato_base = formato_centrado

                        # Escribir celda con el formato correspondiente
                        if pd.isna(valor):
                            worksheet.write_blank(row_idx + 1, col_idx, None, formato_base)
                        elif col_name in columnas_fecha_renombradas:
                            worksheet.write_datetime(row_idx + 1, col_idx, pd.to_datetime(valor), formato_base)
                        elif isinstance(valor, (int, float)):
                            worksheet.write_number(row_idx + 1, col_idx, valor, formato_base)
                        else:
                            worksheet.write(row_idx + 1, col_idx, valor, formato_base)

                # Ajustar ancho de columnas
                worksheet.set_column(0, len(df_pandas.columns) - 1, 15)

                print(f"   - Hoja '{nombre_hoja}' exportada y formateada con éxito.")

        print("\n¡Proceso completado! Archivo final generado.")

    except Exception as e:
        print(f"ERROR al guardar el archivo Excel final: {e}")

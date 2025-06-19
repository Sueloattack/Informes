# main.py
import polars as pl
from ui import dialogs
from core import processor
from settings import config

def main_flow():
    ruta_entrada = dialogs.seleccionar_archivo_entrada()
    if not ruta_entrada: print("Operación cancelada."); return
    df_base = processor.cargar_y_limpiar_base(ruta_entrada)
    if df_base is None or df_base.is_empty(): print("El procesamiento no generó datos."); return
    
    print("\n3. Clasificando facturas en las 5 categorías...")
    con_cc, con_fr = pl.col(config.COL_CUENTA_COBRO) != 0, pl.col(config.COL_FECHA_RADICADO).is_not_null()
    cond_t1, cond_t2, cond_t3, cond_t4 = con_cc & con_fr, con_cc & ~con_fr, ~con_cc & ~con_fr, ~con_cc & con_fr

    items_t1, items_t2, items_t3, items_t4 = processor.obtener_facturas_puras(df_base, cond_t1), processor.obtener_facturas_puras(df_base, cond_t2), processor.obtener_facturas_puras(df_base, cond_t3), processor.obtener_facturas_puras(df_base, cond_t4)
    ids_puras = pl.concat([items_t1.select(config.COLUMNAS_FACTURA), items_t2.select(config.COLUMNAS_FACTURA), items_t3.select(config.COLUMNAS_FACTURA), items_t4.select(config.COLUMNAS_FACTURA)]).unique()
    items_t5 = df_base.join(ids_puras, on=config.COLUMNAS_FACTURA, how="anti")
    
    print("\n--- RECUENTO DE FACTURAS POR CATEGORÍA ---")
    counts = {"T1 (Radicadas OK)": items_t1.select(config.COLUMNAS_FACTURA).n_unique(), "T2 (Con CC, Sin FR)": items_t2.select(config.COLUMNAS_FACTURA).n_unique(), "T3 (Sin CC, Sin FR)": items_t3.select(config.COLUMNAS_FACTURA).n_unique(), "T4 (Sin CC, Con FR)": items_t4.select(config.COLUMNAS_FACTURA).n_unique(), "T5 (Facturas Mixtas)": items_t5.select(config.COLUMNAS_FACTURA).n_unique()}
    total_base, total_calculado = df_base.select(config.COLUMNAS_FACTURA).n_unique(), sum(counts.values())

    print(f"Total de facturas únicas en la base: {total_base}")
    for categoria, count in counts.items(): print(f"  - {categoria:<25}: {count} facturas")
    print("-----------------------------------------------")
    print(f"Suma de facturas en categorías: {total_calculado}")
    if total_base == total_calculado: print("¡Comprobación exitosa! Todas las facturas han sido clasificadas.")
    else: print("¡ADVERTENCIA! La suma no coincide con el total.")
    
    print("\n4. Generando tablas finales con formato Resumen/Detalle...")
    dict_de_tablas = {"1_RadicadasOK": processor.crear_tabla_resumen_detalle(items_t1), "2_ConCC_SinFR": processor.crear_tabla_resumen_detalle(items_t2), "3_SinCC_SinFR": processor.crear_tabla_resumen_detalle(items_t3), "4_SinCC_ConFR": processor.crear_tabla_resumen_detalle(items_t4), "5_Mixtas": processor.crear_tabla_resumen_detalle(items_t5)}
    
    ruta_salida = dialogs.seleccionar_ruta_salida()
    if not ruta_salida: print("Operación cancelada."); return
    processor.exportar_reporte_final(dict_de_tablas, ruta_salida)

if __name__ == "__main__":
    main_flow()
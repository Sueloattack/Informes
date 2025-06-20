# --- Nombre de la hoja a leer ---
NOMBRE_HOJA_POR_DEFECTO = 'Hoja1'

# --- Nombres de Columnas en el Excel Original ---
COLUMNAS_ORIGINALES = [
    'gl_fecha', 'fc_serie', 'fc_docn', 'gl_docn',
    'nom_terce', 'vr_glosa', 'estatus1', 'fecha_gl',
    'gr_docn', 'fecha_rep'
]

# --- Definiciones internas de columnas ---
COL_FECHA_OBJECION = 'gl_fecha'
COL_SERIE = 'fc_serie'
COL_N_FACTURA = 'fc_docn'
COL_GL_DOCN = 'gl_docn'
COL_ENTIDAD = 'nom_terce'
COL_VR_GLOSA = 'vr_glosa'
COL_ESTATUS = 'estatus1'
COL_FECHA_CONTESTACION = 'fecha_gl'
COL_CUENTA_COBRO = 'gr_docn'
COL_FECHA_RADICADO = 'fecha_rep'
COL_FACTURA_CONCAT = 'Factura'

# --- Grupos de Columnas para Procesamiento ---
COLUMNAS_FACTURA = [COL_SERIE, COL_N_FACTURA, COL_GL_DOCN]
COLUMNAS_FECHA = [COL_FECHA_OBJECION, COL_FECHA_CONTESTACION, COL_FECHA_RADICADO]
COLUMNAS_DOCN = [COL_N_FACTURA, COL_GL_DOCN]

# --- Reglas de Negocio ---
ESTATUS_VALIDOS = ['AI', 'C1', 'C2', 'C3', 'CO']
TEXTO_NULO_FECHA = "  -   -"  # El texto exacto a buscar y reemplazar

# --- Mapeo de Nombres para la Exportación Final ---
COLUMN_NAME_MAPPING_EXPORT = {
    COL_SERIE: "Serie",
    COL_N_FACTURA: "N° Factura",
    COL_FACTURA_CONCAT: "Factura",
    COL_GL_DOCN: "No. Paciente (Gl_docn)",
    COL_ENTIDAD: "Entidad",
    COL_FECHA_OBJECION: "Fecha Objeción",
    COL_FECHA_CONTESTACION: "Fecha Contestación",
    COL_FECHA_RADICADO: "Fecha Radicado",
    COL_CUENTA_COBRO: "Cuenta de Cobro",
    COL_ESTATUS: "Estatus",
    COL_VR_GLOSA: "Valor Glosa",
}   

COLUMNAS_FINALES_ORDENADAS = [
    # Identificación
    COL_SERIE,
    COL_N_FACTURA,
    COL_FACTURA_CONCAT,
    COL_GL_DOCN,
    COL_ENTIDAD,
    # Fechas
    COL_FECHA_OBJECION,
    COL_FECHA_CONTESTACION,
    COL_FECHA_RADICADO,
    # Estado y valor
    COL_CUENTA_COBRO,
    COL_ESTATUS,
    COL_VR_GLOSA,
    # Columnas generadas
    "Tipo de Fila",
    "Total Items Factura",
    "Con CC y Con FR",
    "Con CC y Sin FR",
    "Sin CC y Sin FR",
    "Sin CC y Con FR",
]
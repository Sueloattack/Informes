# ui/dialogs.py
import tkinter as tk
from tkinter import filedialog

def seleccionar_archivo_entrada():
    root = tk.Tk()
    root.withdraw()
    return filedialog.askopenfilename(
        title="Selecciona el archivo Excel base",
        filetypes=[("Archivos de Excel", "*.xlsx *.xls")]
    )

def seleccionar_ruta_salida():
    root = tk.Tk()
    root.withdraw()
    return filedialog.asksaveasfilename(
        title="Guardar reporte final como...",
        filetypes=[("Archivo de Excel", "*.xlsx")],
        defaultextension=".xlsx",
        initialfile="Reporte_Clasificado.xlsx"
    )
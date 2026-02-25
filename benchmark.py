import pandas as pd
import polars as pl
import time
import shutil
import statistics
import xlsxwriter
import gc
import io
import contextlib
import duckdb
import pyarrow.parquet as pq
import pyarrow.feather as pf
import pyarrow.csv as pv
from pathlib import Path

# --- Configuración ---
BUCLES = 5
DATASETS_DIR = Path('data')
TEMP_DIR = Path('Temp')
ANALYSIS_FILE = 'results_summary.xlsx'

# Diccionario unificado con funciones lambda
FORMATOS = {
    'pandas': {
        'csv': {'write': lambda df, p: df.to_csv(p, index=False), 'read': pd.read_csv},
        'excel': {'write': lambda df, p: df.to_excel(p, index=False), 'read': pd.read_excel},
        'parquet': {'write': lambda df, p: df.to_parquet(p, index=False), 'read': pd.read_parquet},
        'feather': {'write': lambda df, p: df.to_feather(p), 'read': pd.read_feather},
        'json': {'write': lambda df, p: df.to_json(p, orient='records', lines=True, date_format='iso'), 
                 'read': lambda p: pd.read_json(p, lines=True)}
    },
    'polars': {
        'csv': {'write': lambda df, p: df.write_csv(p), 'read': pl.read_csv},
        'excel': {'write': lambda df, p: df.write_excel(p), 'read': pl.read_excel},
        'parquet': {'write': lambda df, p: df.write_parquet(p), 'read': pl.read_parquet},
        'feather': {'write': lambda df, p: df.write_ipc(p), 'read': pl.read_ipc},
        'json': {'write': lambda df, p: df.write_ndjson(p), 'read': pl.read_ndjson}
    },
    'duckdb': {
        'csv': {'write': lambda df, p: duckdb.query("SELECT * FROM df").write_csv(str(p)), 
                'read': lambda p: duckdb.query(f"SELECT * FROM read_csv_auto('{p}')").df()},
        'parquet': {'write': lambda df, p: duckdb.query("SELECT * FROM df").write_parquet(str(p)), 
                    'read': lambda p: duckdb.query(f"SELECT * FROM read_parquet('{p}')").df()},
        'json': {'write': lambda df, p: duckdb.execute(f"COPY (SELECT * FROM df) TO '{p}' (FORMAT JSON, ARRAY TRUE)"), 
                 'read': lambda p: duckdb.query(f"SELECT * FROM read_json_auto('{p}')").df()}
    },
    'pyarrow': {
        'csv': {'write': lambda df, p: pv.write_csv(pl.from_pandas(df).to_arrow(), str(p)), 
                'read': lambda p: pv.read_csv(str(p)).to_pandas()},
        'parquet': {'write': lambda df, p: pq.write_table(pl.from_pandas(df).to_arrow(), str(p)), 
                    'read': lambda p: pq.read_table(str(p)).to_pandas()},
        'feather': {'write': lambda df, p: pf.write_feather(pl.from_pandas(df).to_arrow(), str(p)), 
                    'read': lambda p: pf.read_feather(str(p))}
    }
}

def media_acotada(datos, proporcion_a_quitar=0.1):
    if not datos: return 0
    n = len(datos)
    quitar = int(n * proporcion_a_quitar)
    if quitar * 2 >= n: return statistics.median(datos)
    datos_acotados = sorted(datos)[quitar:-quitar]
    return (sum(datos_acotados) / len(datos_acotados)) if datos_acotados else statistics.median(datos)

def medir_tiempos(df, lib_name, fmt, ruta, fmt_name, total_bucles):
    tiempos = {'lectura': [], 'escritura': []}
    size_kb = 0
    buffer_warnings = io.StringIO()

    for i in range(total_bucles):
        msg = f"  ⚡ {lib_name.upper()} > {fmt_name.upper()}: Iteración {i + 1}/{total_bucles}"
        print(f"\r{msg}\033[K", end='', flush=True)
        ruta_iter = ruta.with_stem(f"{ruta.stem}_{lib_name}_{i}")
        
        try:
            with contextlib.redirect_stderr(buffer_warnings):
                start = time.perf_counter()
                fmt['write'](df, ruta_iter)
                tiempos['escritura'].append((time.perf_counter() - start) * 1000)
                
                if i == 0: size_kb = ruta_iter.stat().st_size / 1024
                
                start = time.perf_counter()
                fmt['read'](ruta_iter)
                tiempos['lectura'].append((time.perf_counter() - start) * 1000)
            
            gc.collect() 
        except Exception as e:
            print(f"\n❌ Error en {lib_name}/{fmt_name}: {e}")
            tiempos['escritura'].append(0); tiempos['lectura'].append(0)

    warn_text = buffer_warnings.getvalue().strip()
    suffix = f" [Aviso: {warn_text.splitlines()[0]}]" if warn_text else ""
    print(f"\r  ✅ {lib_name.upper()} > {fmt_name.upper()}: Completado.{suffix}\033[K")
    return tiempos, size_kb

def _formatear_resumen(writer, df_pivot):
    # Ordenar y reindexar métricas
    metricas_ordenadas = ['Escritura (ms)', 'Lectura (ms)', 'Tamaño (KB)']
    metricas_presentes = [m for m in metricas_ordenadas if m in df_pivot.columns.levels[0]]
    df_pivot = df_pivot.reindex(columns=metricas_presentes, level=0)
    
    workbook = writer.book
    worksheet = workbook.add_worksheet('Resumen_Comparativo')
    writer.sheets['Resumen_Comparativo'] = worksheet

    # --- Estilos Base ---
    fmt_base = {'border': 1, 'align': 'center', 'valign': 'vcenter'}
    fmt_header = {**fmt_base, 'bold': True}
    
    style_idx_h = workbook.add_format({**fmt_header, 'bg_color': '#BDD7EE'}) 
    style_esc = workbook.add_format({**fmt_header, 'bg_color': '#00B050', 'font_color': 'white'}) 
    style_lec = workbook.add_format({**fmt_header, 'bg_color': '#ED7D31', 'font_color': 'white'}) 
    style_tam = workbook.add_format({**fmt_header, 'bg_color': '#4472C4', 'font_color': 'white'}) 
    style_lib = workbook.add_format({**fmt_header, 'bg_color': '#D9E1F2', 'font_size': 9})
    
    num_fmt = workbook.add_format({**fmt_base, 'num_format': '#,##0.00'})
    idx_data_fmt = workbook.add_format({**fmt_base, 'align': 'left'})

    # --- Encabezados ---
    worksheet.merge_range(0, 0, 1, 0, 'Formato', style_idx_h)
    worksheet.merge_range(0, 1, 1, 1, 'Archivo', style_idx_h)

    libs = df_pivot.columns.levels[1]
    num_libs = len(libs)

    col_actual = 2
    for metric in metricas_presentes:
        style = style_esc if 'Esc' in metric else style_lec if 'Lec' in metric else style_tam
        worksheet.merge_range(0, col_actual, 0, col_actual + num_libs - 1, metric, style)
        
        for i, lib in enumerate(libs):
            worksheet.write(1, col_actual + i, lib.upper(), style_lib)
            
            # Ancho dinámico basado en contenido y cabeceras inferiores
            col_data = df_pivot[(metric, lib)]
            data_max = col_data.apply(lambda x: len(f"{x:,.2f}") if pd.notna(x) else 0).max()
            actual_width = max(len(lib), data_max) + 4
            worksheet.set_column(col_actual + i, col_actual + i, actual_width)
        
        col_actual += num_libs

    # Anchos para las columnas de índice
    for i, col_name in enumerate(['Formato', 'Archivo']):
        data_max = df_pivot.index.get_level_values(i).astype(str).str.len().max()
        actual_width = max(len(col_name), data_max) + 4
        worksheet.set_column(i, i, actual_width)

    # --- Escritura de Datos ---
    for row_idx, (idx_tuple, row_data) in enumerate(df_pivot.iterrows(), start=2):
        worksheet.write(row_idx, 0, idx_tuple[0], idx_data_fmt)
        worksheet.write(row_idx, 1, idx_tuple[1], idx_data_fmt)
        
        for col_idx, val in enumerate(row_data, start=2):
            if pd.notna(val):
                worksheet.write(row_idx, col_idx, val, num_fmt)
            else:
                worksheet.write_blank(row_idx, col_idx, '', num_fmt)

    # --- Formato Condicional y Congelado ---
    worksheet.freeze_panes(2, 2)
    last_row = len(df_pivot) + 1 
    
    c_speed = {'type': '3_color_scale', 'min_color': "#63BE7B", 'mid_color': "#FFEB84", 'max_color': "#F8696B"}

    col_idx = 2
    for metric in metricas_presentes:
        # Se aplica escala de colores solo para métricas de tiempo
        if 'Tamaño' not in metric:
            rango = f"{xlsxwriter.utility.xl_col_to_name(col_idx)}3:{xlsxwriter.utility.xl_col_to_name(col_idx + num_libs - 1)}{last_row + 1}"
            worksheet.conditional_format(rango, c_speed)
        col_idx += num_libs
        
def analizar_archivos():
    """Ejecuta el benchmark y genera el reporte Excel jerárquico."""
    archivos = sorted(list(DATASETS_DIR.glob('*.xlsx')), key=lambda p: p.stat().st_size)
    if not archivos:
        print(f"⚠️  No se encontraron archivos en '{DATASETS_DIR}'.")
        return

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    TEMP_DIR.mkdir(exist_ok=True)
    
    resultados = []
    metric_func = media_acotada if BUCLES >= 5 else statistics.mean

    for archivo in archivos:
        size_mb = archivo.stat().st_size / (1024**2)
        print(f"\n📂 DATASET: {archivo.stem.upper()} ({size_mb:.2f} MB)")
        
        pd_df = pd.read_excel(archivo, engine='openpyxl')
        
        dfs = {
            'pandas': pd_df,
            'polars': pl.from_pandas(pd_df),
            'duckdb': pd_df,
            'pyarrow': pd_df
        }
        
        for lib in FORMATOS.keys():
            df_actual = dfs[lib]
            for fmt_name, fmt_details in FORMATOS[lib].items():
                ext = {'excel': 'xlsx', 'feather': 'arrow'}.get(fmt_name, fmt_name)
                ruta = TEMP_DIR / f"{archivo.stem}.{ext}"
                
                tiempos, size_kb = medir_tiempos(df_actual, lib, fmt_details, ruta, fmt_name, BUCLES)
                
                resultados.append({
                    "Librería": lib.upper(), 
                    "Archivo": archivo.stem, 
                    "Formato": fmt_name.upper(),
                    "Tamaño (KB)": size_kb, 
                    "Lectura (ms)": metric_func(tiempos['lectura']),
                    "Escritura (ms)": metric_func(tiempos['escritura'])
                })

    df_resum = pd.DataFrame(resultados)
    
    df_pivot = df_resum.pivot_table(
        index=['Formato', 'Archivo'], 
        columns='Librería', 
        values=['Escritura (ms)', 'Lectura (ms)', 'Tamaño (KB)']
    )

    print(f"\n📊 Generando reporte jerárquico en {ANALYSIS_FILE}...")
    with pd.ExcelWriter(ANALYSIS_FILE, engine='xlsxwriter') as writer:
        _formatear_resumen(writer, df_pivot)
        df_resum.to_excel(writer, sheet_name="RAW_Data", index=False)
        
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print(f"\n✨ Benchmark finalizado con éxito. Reporte: '{ANALYSIS_FILE}'")

if __name__ == '__main__':
    analizar_archivos()
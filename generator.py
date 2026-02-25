import pandas as pd
import numpy as np
from pathlib import Path
import lorem
from datetime import datetime, timedelta

# --- Configuración de Reproducibilidad ---
# Fijar la semilla asegura que el benchmark sea comparable siempre
np.random.seed(42) 

TIPOS = {
    'num': ('Precio', lambda n: np.random.rand(n) * 100),
    'int': ('Cantidad', lambda n: np.random.randint(0, 1000, size=n)),
    'text': ('Descripción', lambda n: [lorem.sentence() for _ in range(n)]),
    'long_text': ('Notas', lambda n: [lorem.paragraph() for _ in range(n)]),
    'date': ('Fecha', lambda n: [datetime(2023,1,1) + timedelta(days=int(x)) for x in np.random.randint(0, 365, size=n)]),
    'time': ('Hora', lambda n: [str(timedelta(seconds=int(x))) for x in np.random.randint(0, 24*3600, size=n)]),
    'bool': ('Disponible', lambda n: np.random.choice([True, False], size=n)),
    'category': ('Categoría', lambda n: pd.Categorical(np.random.choice(['cat1', 'cat2', 'cat3'], size=n))),
    'null': ('Sin_datos', lambda n: [None] * n),
    'location': ('Ubicación', lambda n: np.random.choice(['Ciudad A', 'Ciudad B', 'Ciudad C'], size=n)),
    'percentage': ('Porcentaje', lambda n: np.random.rand(n) * 100)
}

def crear_datos(filas, columnas, tipos):
    """Genera un DataFrame con tipos de datos cíclicos."""
    data = {}
    for i in range(columnas):
        tipo_key = tipos[i % len(tipos)]
        nombre_base, func_gen = TIPOS[tipo_key]
        data[f"{nombre_base}_{i+1}"] = func_gen(filas)
    return pd.DataFrame(data)

def guardar_excel(dfs, carpeta="data"):
    """Crea el directorio si no existe y guarda los DataFrames."""
    output_dir = Path(carpeta)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for nombre, df in dfs.items():
        archivo = output_dir / f"{nombre}.xlsx"
        df.to_excel(archivo, index=False, engine='xlsxwriter')
        print(f"✅ Archivo generado: {archivo} | Filas: {len(df)}")

def main():
    config_tamaños = {
        'pequeño': (1_000, 12),
        'mediano': (10_000, 24),
        'grande': (100_000, 36)
    }
    
    tipos_disponibles = list(TIPOS.keys())
    
    print("🚀 Iniciando generación de datasets sintéticos...")
    
    # List comprehension para crear el diccionario de DFs
    datasets = {
        nombre: crear_datos(filas, cols, tipos_disponibles) 
        for nombre, (filas, cols) in config_tamaños.items()
    }
    
    print("💾 Persistiendo datos en disco...")
    guardar_excel(datasets)
    print("\n✨ Proceso finalizado. Todo listo para el benchmark.")

if __name__ == "__main__":
    main()
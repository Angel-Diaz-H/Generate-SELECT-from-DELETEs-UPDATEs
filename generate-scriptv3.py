import re
from pathlib import Path
from collections import defaultdict, OrderedDict

def formatear_y_generar_selects(texto_sql: str):
    texto_limpio = re.sub(r"--.*", "", texto_sql)
    texto_unido = " ".join(line.strip() for line in texto_limpio.splitlines() if line.strip())
    sentencias = re.split(r"\b(?=delete|update)\b", texto_unido, flags=re.IGNORECASE)

    # Agrupa por tabla -> columna -> set(valores)
    agrupados = defaultdict(lambda: defaultdict(set))
    # Para mantener el orden de las columnas como aparecen en la primera sentencia
    columnas_orden = defaultdict(list)
    selects_individuales = []

    for sentencia in sentencias:
        sentencia = sentencia.strip()
        if not sentencia:
            continue

        # Detecta tabla para UPDATE (Oracle style)
        m_update = re.match(r"update\s+([a-zA-Z0-9_.]+)\s+set\s+.+?\s+where\s+(.+)", sentencia, re.IGNORECASE)
        # Detecta tabla para DELETE
        m_delete = re.match(r"delete\s+from\s+([a-zA-Z0-9_.]+)\s+where\s+(.+)", sentencia, re.IGNORECASE)

        if m_update:
            tabla = m_update.group(1)
            condiciones = m_update.group(2).strip().rstrip(";")
        elif m_delete:
            tabla = m_delete.group(1)
            condiciones = m_delete.group(2).strip().rstrip(";")
        else:
            continue

        # Soporta condiciones múltiples separadas por AND
        condiciones_lista = [c.strip() for c in re.split(r"\bAND\b", condiciones, flags=re.IGNORECASE)]
        columnas = []
        valores = []
        todas_simples = True

        for cond in condiciones_lista:
            m_cond = re.match(r"([a-zA-Z0-9_]+)\s*=\s*('[^']*'|[0-9]+)", cond)
            if m_cond:
                col = m_cond.group(1)
                val = m_cond.group(2)
                columnas.append(col)
                valores.append(val)
            else:
                todas_simples = False
                break

        if todas_simples and columnas:
            # Guarda el orden de las columnas la primera vez
            if not columnas_orden[tabla]:
                columnas_orden[tabla] = columnas
            for col, val in zip(columnas, valores):
                agrupados[tabla][col].add(val)
        else:
            # Si no es condición simple, genera select individual
            selects_individuales.append(f"select * from {tabla} where {condiciones};")

    selects_generados = []
    for tabla, columnas_dict in agrupados.items():
        # Usa el orden de columnas de la primera aparición
        orden = columnas_orden[tabla] if columnas_orden[tabla] else sorted(columnas_dict.keys())
        condiciones = []
        for col in orden:
            vals = columnas_dict[col]
            # Solución: Ordena todos como string para evitar error de comparación
            lista = ",".join(sorted(vals, key=lambda x: str(x)))
            condiciones.append(f"{col} in ({lista})")
        if condiciones:
            selects_generados.append(f"select * from {tabla} where {' and '.join(condiciones)};")

    selects_generados.extend(selects_individuales)
    return selects_generados

# === EJECUCIÓN PRINCIPAL ===
if __name__ == "__main__":
    ruta_entrada = Path("entrada.sql")
    ruta_salida = Path("script-selects.txt")

    if not ruta_entrada.exists():
        print("⏸️ Archivo de entrada no encontrado.")
    else:
        contenido = ruta_entrada.read_text(encoding="utf-8")
        selects = formatear_y_generar_selects(contenido)

        with ruta_salida.open("w", encoding="utf-8") as f:
            f.write("-- SELECTS DE RESPALDO AGRUPADOS --\n")
            for sel in selects:
                f.write(sel + "\n")

        print(f"✅ Archivo generado correctamente en: {ruta_salida.resolve()}")
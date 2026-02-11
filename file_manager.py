import os
import hashlib
import time
import shutil
from logger import log_message


import csv
import io

class FileManager:
    """Clase para manejar versiones de archivos basadas en hash MD5 del contenido ordenado."""

    @staticmethod
    def calculate_sorted_md5(file_path: str) -> str:
        """
        Calcula un hash MD5 del contenido del CSV ordenado por filas.
        Esto permite ignorar cambios que sean solo de reordenamiento de filas.
        Asume delimitador ';' y encoding utf-8.
        """
        try:
            # Leemos todas las filas en memoria (OK para archivos medianos < 100MB)
            with open(file_path, "r", encoding="utf-8", newline='') as f:
                # Intentar detectar delimitador o usar ';'
                sample = f.read(1024)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    delimiter = dialect.delimiter
                except:
                    delimiter = ';'
                
                reader = csv.reader(f, delimiter=delimiter)
                rows = list(reader)

            if not rows:
                return hashlib.md5(b"").hexdigest()

            # Separar header y datos
            header = rows[0]
            data = rows[1:]

            # Identificar índice de columna "Item" para excluirla (pues cambia con el orden)
            try:
                item_idx = header.index("Item")
                # Eliminar campo Item del header
                header.pop(item_idx)
                # Eliminar campo Item de cada fila
                for row in data:
                    if len(row) > item_idx:
                        row.pop(item_idx)
            except ValueError:
                pass  # Si no existe Item, continuar normal

            # Ordenar datos (ahora sin el Item que ensuciaba el orden)
            data.sort()

            # Reconstruir el contenido ordenado en memoria
            output = io.StringIO()
            writer = csv.writer(output, delimiter=delimiter, lineterminator='\n')
            writer.writerow(header)
            writer.writerows(data)
            
            # Calcular MD5 del string resultante
            content_bytes = output.getvalue().encode('utf-8')
            return hashlib.md5(content_bytes).hexdigest()

        except Exception as e:
            log_message(f"[WARN] Error calculando hash ordenado para {file_path}: {e}")
            # Fallback al hash de archivo crudo si falla el parseo CSV
            return FileManager.calculate_raw_md5(file_path)

    @staticmethod
    def calculate_raw_md5(file_path: str) -> str:
        """Calcula el hash MD5 binario del archivo (método antiguo)."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def manage_versioning(new_file_path: str, target_dir: str, target_filename: str):
        """
        Gestiona la version final del archivo.
        Si ya existe uno con el mismo nombre en target_dir:
          1. Compara MD5.
          2. Si son diferentes:
             - Renombra el existente a {target_filename}_{timestamp}.csv
             - Mueve el nuevo a {target_filename}
          3. Si son iguales:
             - Borra el nuevo (no hay cambios).
        Si no existe:
          - Simplemente mueve el nuevo a la ubicación final.
        """
        final_path = os.path.join(target_dir, target_filename)

        if not os.path.exists(final_path):
            log_message(f"[INFO] No existe archivo previo. Creando {final_path}")
            shutil.move(new_file_path, final_path)
            return

        # Calcular hashes (usando contenido ordenado para ignorar diferencias de orden)
        new_md5 = FileManager.calculate_sorted_md5(new_file_path)
        old_md5 = FileManager.calculate_sorted_md5(final_path)

        if new_md5 == old_md5:
            log_message("[INFO] El archivo descargado es IDENTICO al actual. Se descarta el nuevo.")
            os.remove(new_file_path)
        else:
            log_message("[INFO] El archivo ha CAMBIADO.")
            
            # Timestamp para el backup
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            name, ext = os.path.splitext(target_filename)
            backup_filename = f"{name}_{timestamp}{ext}"
            backup_path = os.path.join(target_dir, backup_filename)
            
            # Renombrar el antiguo -> backup
            try:
                os.rename(final_path, backup_path)
                log_message(f"[OK] Archivo antiguo respaldado como: {backup_filename}")
            except OSError as e:
                log_message(f"[ERROR] No se pudo respaldar el archivo antiguo: {e}")
                # En caso de error, quizas queramos abortar o forzar.
                # Por seguridad, abortamos para no perder data.
                return

            # Mover el nuevo -> final
            shutil.move(new_file_path, final_path)
            log_message(f"[OK] Archivo nuevo actualizado como: {target_filename}")

    @staticmethod
    def _save_audit_file(rows, target_dir, base_filename, header, delimiter=';'):
        """Helper para guardar archivos de auditoría con versionado."""
        if not rows:
            return

        final_path = os.path.join(target_dir, base_filename)
        
        # Versionar anterior si existe
        if os.path.exists(final_path):
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            name, ext = os.path.splitext(base_filename)
            backup_name = f"{name}_{timestamp}{ext}"
            backup_path = os.path.join(target_dir, backup_name)
            try:
                os.rename(final_path, backup_path)
            except OSError as e:
                log_message(f"[WARN] No se pudo versionar {base_filename}: {e}")

        # Guardar nuevo
        try:
            with open(final_path, "w", encoding="utf-8", newline='') as f:
                writer = csv.writer(f, delimiter=delimiter, lineterminator='\n', quoting=csv.QUOTE_ALL)
                writer.writerow(header)
                writer.writerows(rows)
            log_message(f"[INFO] Generado reporte de auditoría: {base_filename} ({len(rows)} filas)")
        except Exception as e:
            log_message(f"[ERROR] Falló al guardar {base_filename}: {e}")

    @staticmethod
    def compare_and_extract_diffs(new_file_path: str, target_dir: str, target_filename: str):
        """
        Compara el archivo nuevo con el existente.
        Separando:
         - _Diff.csv: Filas agregadas o modificadas.
         - _Del.csv: Filas eliminadas (estaban antes, ahora no).
        Mantiene versionado rotativo para estos archivos.
        """
        final_path = os.path.join(target_dir, target_filename)
        
        if not os.path.exists(final_path):
            return {'diff': None, 'del': None}

        try:
            # Leer archivo viejo
            old_rows = {}
            header = []
            delimiter = ';' # Asumimos punto y coma por consistencia
            
            with open(final_path, "r", encoding="utf-8", newline='') as f:
                reader = csv.reader(f, delimiter=delimiter)
                try:
                    header = next(reader)
                except StopIteration:
                    return {'diff': None, 'del': None} # Archivo vacio
                
                # Buscar indices
                try:
                    id_idx = header.index("IdVentaNegocioDetalle")
                except ValueError:
                    log_message("[WARN] No se encontró IdVentaNegocioDetalle en archivo existente.")
                    return {'diff': None, 'del': None}

                for row in reader:
                    if len(row) > id_idx:
                        id_val = row[id_idx]
                        old_rows[id_val] = row

            # Leer archivo nuevo y buscar cambios
            diff_rows = [] # Agregadas o Modificadas
            seen_ids = set()
            
            with open(new_file_path, "r", encoding="utf-8", newline='') as f:
                reader = csv.reader(f, delimiter=delimiter)
                try:
                    new_header = next(reader)
                except StopIteration:
                    return {'diff': None, 'del': None} # Nuevo vacio

                # Validar headers iguales (idealmente)
                if new_header != header:
                    log_message("[WARN] Las cabeceras difieren, se intentará procesar igual.")
                
                try:
                    new_id_idx = new_header.index("IdVentaNegocioDetalle")
                    # Indice de 'Item' para ignorar en comparación
                    try:
                        item_idx = new_header.index("Item")
                    except ValueError:
                        item_idx = -1
                except ValueError:
                    log_message("[WARN] No se encontró IdVentaNegocioDetalle en archivo nuevo.")
                    return {'diff': None, 'del': None}


                for row in reader:
                    if len(row) <= new_id_idx:
                        continue
                        
                    id_val = row[new_id_idx]
                    seen_ids.add(id_val)
                    
                    # Logica de comparacion
                    if id_val not in old_rows:
                        # NUEVA FILA
                        diff_rows.append(row)
                    else:
                        # FILA EXISTENTE - COMPARAR CONTENIDO
                        old_row = old_rows[id_val]
                        
                        # Copias para comparar sin 'Item'
                        row_to_compare = list(row)
                        old_row_to_compare = list(old_row)
                        
                        if item_idx != -1:
                            if len(row_to_compare) > item_idx:
                                row_to_compare.pop(item_idx)
                            if len(old_row_to_compare) > item_idx:
                                old_row_to_compare.pop(item_idx)
                        
                        if row_to_compare != old_row_to_compare:
                            # MODIFICADA
                            diff_rows.append(row)

            # Identificar filas ELIMINADAS (estaban en old, no en new)
            del_rows = []
            for old_id, old_row in old_rows.items():
                if old_id not in seen_ids:
                    del_rows.append(old_row)

            # Guardar reportes usando el helper versionador
            base_name = os.path.splitext(target_filename)[0]
            
            paths = {'diff': None, 'del': None}

            # 1. Guardar Diffs (Agregadas/Modificadas)
            if diff_rows:
                diff_name = f"{base_name}_Diff.csv"
                FileManager._save_audit_file(diff_rows, target_dir, diff_name, header)
                paths['diff'] = os.path.join(target_dir, diff_name)
            
            # 2. Guardar Eliminadas
            if del_rows:
                del_name = f"{base_name}_Del.csv"
                FileManager._save_audit_file(del_rows, target_dir, del_name, header)
                paths['del'] = os.path.join(target_dir, del_name)

            return paths

        except Exception as e:
            log_message(f"[ERROR] Falló la extracción de diferencias: {e}")
            return {'diff': None, 'del': None}

    @staticmethod
    def rename_to_error(file_paths: list):
        """Mueve archivos a _ERR_{timestamp}.csv"""
        if not file_paths:
            return
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        for path in file_paths:
            if not path or not os.path.exists(path):
                continue
            
            dir_name = os.path.dirname(path)
            base_name = os.path.basename(path)
            name, ext = os.path.splitext(base_name)
            
            # Formato: Nombre_ERR_TIMESTAMP.csv
            new_name = f"{name}_ERR_{timestamp}{ext}"
            new_path = os.path.join(dir_name, new_name)
            
            try:
                os.rename(path, new_path)
                log_message(f"[WARN] Archivo marcado como error: {new_name}")
            except OSError as e:
                log_message(f"[ERROR] No se pudo renombrar a error: {path} -> {e}")

    @staticmethod
    def save_temp_as_error(temp_path: str, target_dir: str, target_filename: str):
        """Guarda el archivo temporal como _ERR (sin reemplazar el oficial)."""
        if not os.path.exists(temp_path):
            return

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(target_filename)
        err_filename = f"{name}_ERR_{timestamp}{ext}"
        final_path = os.path.join(target_dir, err_filename)
        
        try:
            shutil.move(temp_path, final_path)
            log_message(f"[WARN] Archivo principal guardado como error: {err_filename}")
        except OSError as e:
            log_message(f"[ERROR] No se pudo guardar archivo de error: {e}")

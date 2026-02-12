#!/usr/bin/env python3
"""
Bot Turismo Zahr - Punto de entrada.
"""

import sys
import os
from erp_client import ERPClient
from file_manager import FileManager
from db_client import DBClient
from logger import log_message
from dotenv import load_dotenv


def main():
    load_dotenv()
    # IMPORTANTE: Usa las credenciales reales aquí
    USER = os.getenv("ERP_USER")
    PASS = os.getenv("ERP_PASS")

    # Fechas del reporte (formato DD/MM/YYYY)
    FECHA_INICIO = "01/01/2026"
    FECHA_TERMINO = "01/02/2026"
    
    # Nombres de archivo
    TEMP_FILENAME = "download_temp.csv"
    TARGET_FILENAME = "ReporteSabana.csv"

    # Configuración Base de Datos
    DB_SERVER = os.getenv("DB_SERVER")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_DRIVER = os.getenv("DB_DRIVER", "SQL Server")
    DB_PATH_DIFF = os.getenv("DB_PATH_DIFF")
    DB_PATH_DEL = os.getenv("DB_PATH_DEL")

    bot = ERPClient()

    log_message("=== INICIANDO BOT TURISMO ZAHR ===")

    # 1. Handshake inicial
    if not bot.init_session():
        sys.exit(1)

    # 2. Proceso de Login completo (Post + Refresh)
    if not bot.login(USER, PASS):
        log_message("!!! Error fatal en login.")
        sys.exit(1)

    try:
        log_message("=== SISTEMA LISTO ===")
        log_message(f"Usuario {bot.get_user_id()} autenticado.")

        # 3. Descargar reporte CSV (a temporal)
        temp_path = bot.download_report(
            FECHA_INICIO, 
            FECHA_TERMINO, 
            output_filename=TEMP_FILENAME
        )
        
        if temp_path:
            log_message("=== DESCARGA EXITOSA, PROCESANDO ARCHIVO ===")
            
            # Reconstruir el nombre de la carpeta (misma lógica que en ERPClient)
            folder_name = "ReporteSabana"
            
            # 1. Extraer diferencias si aplica (Antes de mover el archivo)
            diff_paths = FileManager.compare_and_extract_diffs(temp_path, folder_name, TARGET_FILENAME)
            
            # Ejecutar SP si hubo cambios detectados (al menos uno de los archivos existe)
            success = True
            if diff_paths['diff'] or diff_paths['del']:
                log_message("=== CAMBIOS DETECTADOS - ACTUALIZANDO BASE DE DATOS ===")
                db = DBClient(DB_SERVER, DB_NAME, DB_USER, DB_PASS, DB_DRIVER)
                if db.connect():
                    # Usar rutas del .env (accesibles desde el SQL Server remoto)
                    path_diff = DB_PATH_DIFF if diff_paths['diff'] else None
                    path_del = DB_PATH_DEL if diff_paths['del'] else None

                    log_message(f"RUTA DIFF: {path_diff}")
                    log_message(f"RUTA DEL: {path_del}")
                    
                    if not db.execute_sp_carga(FECHA_INICIO, FECHA_TERMINO, path_diff, path_del):
                        success = False
                        log_message("!!! La actualización en DB falló.")
                    
                    db.close()
                else:
                    success = False
                    log_message("!!! No se pudo conectar a la DB.")
            else:
                log_message("=== SIN CAMBIOS DETECTADOS - OMITIENDO DB ===")

            if success:
                # 2. Gestionar versionado (solo si todo OK)
                FileManager.manage_versioning(temp_path, folder_name, TARGET_FILENAME)
            else:
                log_message("!!! Ocurrió un error. Guardando archivos como _ERR y cancelando actualización.")
                
                # Renombrar Diffs y Dels generados a _ERR
                files_to_flag = []
                if diff_paths.get('diff'): files_to_flag.append(diff_paths['diff'])
                if diff_paths.get('del'): files_to_flag.append(diff_paths['del'])
                
                FileManager.rename_to_error(files_to_flag)
                
                # Guardar temp como ERR
                FileManager.save_temp_as_error(temp_path, folder_name, TARGET_FILENAME)
            
        else:
            log_message("!!! Error al descargar reporte.")
            sys.exit(1)

    finally:
        # 4. Cerrar sesion
        log_message("=== CERRANDO SESION ===")
        bot.logout()


if __name__ == "__main__":
    main()


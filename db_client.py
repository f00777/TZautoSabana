import os
import pyodbc
from logger import log_message

class DBClient:
    def __init__(self, server, database, user, password):
        self.conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password}"
        )
        self.conn = None

    def connect(self):
        try:
            # autocommit=True ayuda a capturar prints y evitar transacciones colgadas
            self.conn = pyodbc.connect(self.conn_str, autocommit=True)
            log_message("[DB] Conexión a base de datos exitosa.")
            return True
        except Exception as e:
            log_message(f"[DB ERROR] Error de conexión: {e}")
            return False

    def execute_sp_carga(self, fecha_inicio, fecha_termino, path_diff, path_del):
        if not self.conn:
            log_message("[DB] No hay conexión activa.")
            return False

        cursor = self.conn.cursor()
        
        # Manejar NULL para rutas vacías
        p_diff = f"'{path_diff}'" if path_diff else "NULL"
        p_del = f"'{path_del}'" if path_del else "NULL"

        sql = f"""
        EXEC sp_CargaSabanaCompleta 
            @FechaInicio = '{fecha_inicio}', 
            @FechaTermino = '{fecha_termino}', 
            @RutaArchivoDiff = {p_diff}, 
            @RutaArchivoDel = {p_del}
        """
        
        log_message(f"[DB] Ejecutando SP: {sql.strip()}")

        try:
            cursor.execute(sql)
            
            # Intentar consumir todos los resultados para disparar los PRINTs y errores
            # En pyodbc, los mensajes PRINT suelen acumularse en la conexión o cursor.
            # Al iterar nextset(), forzamos la ejecución completa.
            rows_processed = 0
            while True:
                try:
                    # Verificar si hay filas (si el SP hace SELECT)
                    if cursor.description:
                        rows = cursor.fetchall()
                        rows_processed += len(rows)
                except pyodbc.ProgrammingError:
                    # No results, just commands
                    pass
                
                if not cursor.nextset():
                    break
            
            log_message(f"[DB] Ejecución finalizada.")
            
            # Imprimir mensajes del servidor (PRINT / Errors capturados pero no fatales)
            if hasattr(cursor, 'messages') and cursor.messages:
                log_message("--- LOG PROCESO SQL ---")
                for msg in cursor.messages:
                    if isinstance(msg, tuple):
                        log_message(f"[SQL Output] {msg[1]}") # (SQLSTATE, Message)
                    else:
                        log_message(f"[SQL Output] {msg}")
                log_message("-----------------------")
            
            return True

        except pyodbc.Error as e:
            log_message(f"[DB ERROR] Fallo durante la ejecución del SP:")
            # pyodbc errors are often (SQLSTATE, MSG)
            if len(e.args) > 1:
                log_message(f"[SQL Error Details] {e.args[1]}")
            else:
                log_message(f"[SQL Error] {e}")
            return False
        finally:
            cursor.close()

    def close(self):
        if self.conn:
            self.conn.close()
            log_message("[DB] Conexión cerrada.")

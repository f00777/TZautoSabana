#!/usr/bin/env python3
"""
Test de conexión a SQL Server.
Ejecutar: python test_db.py
"""

import os
import sys
import pyodbc
from dotenv import load_dotenv

load_dotenv()

def main():
    # 1. Mostrar drivers ODBC disponibles
    print("=" * 50)
    print("DRIVERS ODBC DISPONIBLES:")
    print("=" * 50)
    drivers = pyodbc.drivers()
    for d in drivers:
        print(f"  - {d}")
    
    if not drivers:
        print("  ❌ No hay drivers ODBC instalados!")
        sys.exit(1)
    
    print()

    # 2. Leer configuración desde .env
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    driver = os.getenv("DB_DRIVER", "SQL Server")

    print("=" * 50)
    print("CONFIGURACIÓN DE CONEXIÓN:")
    print("=" * 50)
    print(f"  Driver:   {driver}")
    print(f"  Server:   {server}")
    print(f"  Database: {database}")
    print(f"  User:     {user}")
    print(f"  Password: {'*' * len(password) if password else '(vacío)'}")
    print()

    # Verificar que el driver existe
    if driver not in drivers:
        print(f"  ⚠️  ADVERTENCIA: El driver '{driver}' NO está en la lista de drivers instalados!")
        print(f"     Drivers disponibles: {', '.join(drivers)}")
        print()

    # 3. Intentar conexión
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password}"
    )

    print("=" * 50)
    print("INTENTANDO CONEXIÓN...")
    print("=" * 50)
    print(f"  Connection String: DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD=****")
    print()

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
        print("  ✅ CONEXIÓN EXITOSA!")
        print()

        # 4. Ejecutar SELECT 1
        print("=" * 50)
        print("EJECUTANDO SELECT 1...")
        print("=" * 50)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS test_result")
        row = cursor.fetchone()
        print(f"  Resultado: {row[0]}")
        
        if row[0] == 1:
            print("  ✅ SELECT 1 EXITOSO - La conexión funciona correctamente!")
        else:
            print("  ⚠️  Resultado inesperado")

        # 5. Info adicional del servidor
        print()
        print("=" * 50)
        print("INFO DEL SERVIDOR:")
        print("=" * 50)
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        print(f"  {version[0][:100]}...")

        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()
        print(f"  Base de datos actual: {db_name[0]}")

        cursor.close()
        conn.close()
        print()
        print("  ✅ Conexión cerrada correctamente.")

    except pyodbc.Error as e:
        print(f"  ❌ ERROR DE CONEXIÓN:")
        print(f"     {e}")
        print()
        print("  POSIBLES SOLUCIONES:")
        print(f"    1. Verificar que el driver '{driver}' esté instalado")
        print(f"    2. Verificar que el servidor '{server}' sea accesible")
        print(f"    3. Verificar credenciales (usuario: {user})")
        print(f"    4. Verificar que el firewall permita la conexión al puerto 1433")
        print(f"    5. Si usas \\sqlexpress, verificar que SQL Server Browser esté activo")
        sys.exit(1)

    print()
    print("=" * 50)
    print("🎉 TODAS LAS PRUEBAS PASARON CORRECTAMENTE")
    print("=" * 50)


if __name__ == "__main__":
    main()

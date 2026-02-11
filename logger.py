import sys
from datetime import datetime

def log_message(message: str, to_console: bool = True):
    """
    Escribe un mensaje en logs.txt con timestamp.
    Opcionalmente imprime en consola.
    """
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    formatted_message = f"{timestamp} {message}"
    
    try:
        with open("logs.txt", "a", encoding="utf-8") as f:
            f.write(formatted_message + "\n")
    except Exception as e:
        # Fallback incase file write fails
        print(f"[LOG ERROR] No se pudo escribir en logs.txt: {e}")

    if to_console:
        print(formatted_message)

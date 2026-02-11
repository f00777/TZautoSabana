import requests
import json
import os
from urllib.parse import quote
from logger import log_message


class ERPClient:
    """
    Cliente ERP para Turismo Zahr.
    Replica el flujo de autenticación de la versión C++:
      PASO 1: GET /  → Obtener cookie de sesión (ASP.NET_SessionId)
      PASO 2: POST /servicios/...IniciarSesion  → Enviar credenciales JSON
      PASO 3: GET /  → Refresh para capturar cookie .ASPXAUTH
    """

    BASE_URL = "http://zahr.asinco.cl"
    LOGIN_ENDPOINT = "/servicios/servicios_sigav.aspx/IniciarSesion"
    LOGOUT_ENDPOINT = "/servicios/servicios_sigav.aspx/CerrarSesion"
    TIMEOUT = 10  # segundos

    def __init__(self):
        self.session = requests.Session()
        self.current_user_id = None
        self._setup_common_headers()

    def _setup_common_headers(self):
        """Headers base para simular navegador."""
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "es-419,es;q=0.9",
        })

    def _print_cookies(self, response, step_name):
        """Imprime las cookies recibidas en un paso dado."""
        log_message(f"--- Cookies recibidas en: {step_name} ---")
        for cookie in response.cookies:
            log_message(f"[COOKIE] {cookie.name} = {cookie.value}")
        log_message("-------------------------------------------")

    # ------------------------------------------------------------------
    # PASO 1: GET Inicial (Handshake)
    # ------------------------------------------------------------------
    def init_session(self) -> bool:
        """Realiza un GET a la raíz para obtener la cookie de sesión inicial."""
        log_message("[PASO 1] Iniciando handshake (GET /)...")

        try:
            response = self.session.get(
                f"{self.BASE_URL}/",
                timeout=self.TIMEOUT,
            )
        except requests.RequestException as e:
            log_message(f"[ERROR] Fallo conexion inicial: {e}")
            return False

        if response.status_code == 200:
            self._print_cookies(response, "Handshake Inicial")
            return True

        log_message(f"[ERROR] Fallo conexion inicial: {response.status_code}")
        return False

    # ------------------------------------------------------------------
    # PASO 2 y 3: Login POST + Refresh GET
    # ------------------------------------------------------------------
    def login(self, email: str, password: str) -> bool:
        """
        Envía credenciales al ERP y luego refresca la sesión
        para capturar la cookie .ASPXAUTH.
        """
        log_message("[PASO 2] Enviando credenciales (POST)...")

        # --- SUB-PASO 2.1: Preparar POST ---
        payload = {
            "Usuario": email,
            "Password": password,
        }

        # Headers ESPECÍFICOS para el Login JSON
        login_headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.BASE_URL}/",
            "Origin": self.BASE_URL,
        }

        try:
            r_login = self.session.post(
                f"{self.BASE_URL}{self.LOGIN_ENDPOINT}",
                json=payload,
                headers=login_headers,
                timeout=self.TIMEOUT,
            )
        except requests.RequestException as e:
            log_message(f"[ERROR] HTTP Login fallido: {e}")
            return False

        if r_login.status_code != 200:
            log_message(f"[ERROR] HTTP Login fallido: {r_login.status_code}")
            return False

        # --- SUB-PASO 2.2: Validar Respuesta JSON ---
        login_success = False
        try:
            json_resp = r_login.json()
            if "d" in json_resp and json_resp["d"]:
                error = json_resp["d"][0].get("ws_Error", "")
                if not error:
                    self.current_user_id = json_resp["d"][0].get("ws_Usuario", "0")
                    login_success = True
                    log_message(f"[OK] Credenciales validas. ID Usuario: {self.current_user_id}")
                else:
                    log_message(f"[FAIL] ERP rechazo login: {error}")
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            log_message(f"[ERROR] JSON invalido: {e}")
            return False

        if not login_success:
            return False

        # --- PASO 3: REFRESCAR COOKIES (GET /) ---
        # Este paso es crucial para obtener .ASPXAUTH
        log_message("[PASO 3] Refrescando sesion para obtener Auth Cookie (GET /)...")

        # IMPORTANTE: Restaurar headers de "navegador normal"
        self._setup_common_headers()

        try:
            r_refresh = self.session.get(
                f"{self.BASE_URL}/",
                timeout=self.TIMEOUT,
            )
        except requests.RequestException as e:
            log_message(f"[ERROR] Refresh fallido: {e}")
            return False

        if r_refresh.status_code == 200:
            # PASO 4: Imprimir Cookies Finales
            self._print_cookies(r_refresh, "Refresh Final (Auth)")

            # Verificación extra
            if self.session.cookies.get(".ASPXAUTH"):
                log_message(">>> AUTENTICACION COMPLETA: Cookie .ASPXAUTH capturada.")
            else:
                log_message(
                    ">>> ADVERTENCIA: No veo .ASPXAUTH en la respuesta "
                    "explicita (podria estar ya en el jar)."
                )

            return True

        return False

    # ------------------------------------------------------------------
    # PASO 4: Descargar Reporte CSV
    # ------------------------------------------------------------------
    def download_report(
        self,
        fecha_inicio: str,
        fecha_termino: str,
        output_filename: str = "ReporteSabana.csv",
    ) -> str | None:
        """
        Descarga el reporte CSV de DescargaCsv.aspx.

        Returns:
            Path completo del archivo descargado si es exitoso, None si falla.
        """
        log_message(f"[PASO 4] Descargando reporte CSV ({fecha_inicio} - {fecha_termino})...")

        # Construir el parámetro tal como aparece en la URL original:
        # sp_InfoReporteSabana '01/10/2025','01/01/2026','0','0',61
        parametros_raw = (
            f"sp_InfoReporteSabana "
            f"'{fecha_inicio}','{fecha_termino}','0','0',61"
        )

        url = (
            f"{self.BASE_URL}/DescargaCsv.aspx"
            f"?parametros={quote(parametros_raw)}"
            f"&info=ReporteSabana"
        )

        log_message(f"[URL] {url}")

        # Restaurar headers de navegador
        self._setup_common_headers()

        try:
            response = self.session.get(
                url,
                timeout=60,  # Timeout más largo para descarga de archivos
                stream=True,
            )
        except requests.RequestException as e:
            log_message(f"[ERROR] Fallo descarga CSV: {e}")
            return None

        if response.status_code != 200:
            log_message(f"[ERROR] HTTP descarga fallida: {response.status_code}")
            return None

        # Crear carpeta de destino: ReporteSabana (folder fijo)
        folder_name = "ReporteSabana"

        os.makedirs(folder_name, exist_ok=True)

        # Guardar archivo CSV
        file_path = os.path.join(folder_name, output_filename)

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size = os.path.getsize(file_path)
        log_message(f"[OK] CSV guardado en: {file_path} ({file_size:,} bytes)")
        return file_path

    # ------------------------------------------------------------------
    # PASO 5: Cerrar Sesion
    # ------------------------------------------------------------------
    def logout(self) -> bool:
        """
        Cierra la sesión enviando un POST a CerrarSesion.
        Usa self.current_user_id para el payload.
        """
        if not self.current_user_id:
            log_message("[WARN] No hay usuario logueado para cerrar sesion.")
            return True

        log_message(f"[PASO 5] Cerrando sesion para usuario {self.current_user_id}...")

        payload = {
            "Usuario": self.current_user_id
        }

        # Headers ESPECÍFICOS para CerrarSesion, similares a Login
        logout_headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.BASE_URL}/",
            "Origin": self.BASE_URL,
        }

        try:
            r_logout = self.session.post(
                f"{self.BASE_URL}{self.LOGOUT_ENDPOINT}",
                json=payload,
                headers=logout_headers,
                timeout=self.TIMEOUT,
            )
        except requests.RequestException as e:
            log_message(f"[ERROR] Fallo cierre de sesion: {e}")
            return False

        if r_logout.status_code == 200:
            log_message("[OK] Sesion cerrada correctamente.")
            self.current_user_id = None
            return True
        
        log_message(f"[ERROR] Fallo cierre de sesion: {r_logout.status_code}")
        return False

    # ------------------------------------------------------------------
    # Getter
    # ------------------------------------------------------------------
    def get_user_id(self) -> str:
        """Retorna el ID del usuario autenticado."""
        return self.current_user_id


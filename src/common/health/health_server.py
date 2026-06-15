import os
import socket
import threading
import logging

logger = logging.getLogger(__name__)

class HealthCheckServer:
    """
    Mixin. Agrega un servidor TCP que responde b'OK\n' a cualquier conexión.
    El monitor usa esto para verificar si el worker está vivo (sin docker).
    """

    def start_health_server(self, port: int | None = None):
        port = port or int(os.environ.get("HEALTH_PORT", "8888"))
        t = threading.Thread(target=self._health_loop, args=(port,), daemon=True)
        t.start()
        logger.info(f"Health server on :{port}")

    def _health_loop(self, port: int):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(("0.0.0.0", port))
            srv.listen(10)
            srv.settimeout(1.0)
            while True:
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break 

                try:
                    conn.sendall(b"OK\n")
                except OSError:
                    pass 
                finally:
                    try:
                        conn.close()
                    except OSError:
                        pass
        finally:
            srv.close()
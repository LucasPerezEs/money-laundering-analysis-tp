"""
Chang-Roberts ring election.
Cada nodo tiene una lista de sucesores en orden: si el primero está caído,
intenta con el siguiente (tolerancia a fallos en el propio anillo).
"""
import os
import socket
import threading
import time
import logging

logger = logging.getLogger(__name__)

ELECTION_PORT      = int(os.environ.get("ELECTION_PORT", "8889"))
HEARTBEAT_TIMEOUT  = float(os.environ.get("HEARTBEAT_TIMEOUT", "9.0"))
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "3"))


class RingElection:
    """
    my_id      : ID único de este monitor (el más alto gana).
    successors : lista ordenada de hostnames de sucesores (primero = sucesor directo).
    """

    def __init__(self, my_id: int, successors: list[str]):
        self.my_id       = my_id
        self.successors  = successors
        self.leader_id   = None
        self._in_election = False
        self._last_hb    = time.time()
        self._lock       = threading.Lock()

    # ── Interfaz pública ──────────────────────────────────────────────────────

    def is_leader(self) -> bool:
        with self._lock:
            return self.leader_id == self.my_id

    def run(self):
        threading.Thread(target=self._server_loop, daemon=True).start()
        time.sleep(2)          # dar tiempo a que todos arranquen
        self._start_election()

        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            if self.is_leader():
                self._broadcast_hb()
            else:
                with self._lock:
                    elapsed = time.time() - self._last_hb
                if elapsed > HEARTBEAT_TIMEOUT:
                    logger.warning(f"[RING] Sin HB en {elapsed:.1f}s -> nueva elección")
                    self._start_election()

    # ── Envío ─────────────────────────────────────────────────────────────────

    def _start_election(self):
        with self._lock:
            self._in_election = True
        self._send(f"ELECTION:{self.my_id}")

    def _send(self, msg: str):
        """Envía al primer sucesor disponible (skip si está caído)."""
        data = (msg + "\n").encode()
        for host in self.successors:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2.0)
                s.connect((host, ELECTION_PORT))
                s.sendall(data)
                s.close()
                return
            except Exception:
                continue
        logger.error("[RING] Ningún sucesor disponible.")

    def _broadcast_hb(self):
        for host in self.successors:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1.0)
                s.connect((host, ELECTION_PORT))
                s.sendall(b"HB\n")
                s.close()
            except Exception:
                pass

    # ── Servidor ──────────────────────────────────────────────────────────────

    def _server_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", ELECTION_PORT))
        srv.listen(10)
        logger.info(f"[RING] Escuchando en :{ELECTION_PORT}")
        while True:
            try:
                conn, _ = srv.accept()
                data = conn.recv(256).decode().strip()
                conn.close()
                threading.Thread(target=self._handle, args=(data,), daemon=True).start()
            except Exception as e:
                logger.error(f"[RING] server error: {e}")

    def _handle(self, msg: str):
        if msg.startswith("HB"):
            with self._lock:
                self._last_hb = time.time()

        elif msg.startswith("ELECTION:"):
            cid = int(msg.split(":")[1])
            if cid > self.my_id:
                self._send(msg)                        # reenviar al sucesor
            elif cid < self.my_id:
                if not self._in_election:
                    with self._lock:
                        self._in_election = True
                    self._send(f"ELECTION:{self.my_id}")  # proponer el propio ID
                # else: ya estamos en elección con nuestro ID -> descartamos
            else:
                # llegó nuestro propio ID -> somos el líder
                with self._lock:
                    self.leader_id   = self.my_id
                    self._in_election = False
                    self._last_hb    = time.time()
                logger.info(f"[RING] ✓ Soy el líder (ID={self.my_id})")
                self._send(f"COORDINATOR:{self.my_id}")

        elif msg.startswith("COORDINATOR:"):
            lid = int(msg.split(":")[1])
            with self._lock:
                self.leader_id   = lid
                self._in_election = False
                self._last_hb    = time.time()
            logger.info(f"[RING] Líder = {lid}")
            if lid != self.my_id:
                self._send(msg)    # propagar al resto del anillo
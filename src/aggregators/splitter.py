"""
Splitter generico configurable por variables de entorno.
Sobreescribe _routing_key para decidir a que particion va cada mensaje.
Stateless: escala libremente.

Variables de entorno:
  SHARD_KEY_FIELD: campo del mensaje usado como clave de sharding
  SHARD_KEY_FIELDS: campos separados por coma (se concatenan como clave)
  NORMALIZE_NUMERIC_KEY: quita ceros iniciales de SHARD_KEY_FIELD
  TAG_SOURCE: si se define, agrega {"source": TAG_SOURCE} a cada msg
"""
import logging
import os
import zlib

from common.middleware.worker_base import WorkerBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Splitter(WorkerBase):

    def __init__(self):
        super().__init__()
        single = os.environ.get("SHARD_KEY_FIELD", "")
        multi  = os.environ.get("SHARD_KEY_FIELDS", "")
        if multi:
            self._key_fields = [f.strip() for f in multi.split(",") if f.strip()]
            self._key_field = None
            self._extract_bytes = lambda msg: "".join(str(msg.get(f, "")) for f in self._key_fields).encode("utf-8")
        elif single:
            self._key_field = single.strip()
            self._extract_bytes = lambda msg: str(msg.get(self._key_field, "")).encode("utf-8")
        else:
            raise ValueError("Se requiere SHARD_KEY_FIELD o SHARD_KEY_FIELDS")
        self._normalize_numeric_key = os.environ.get("NORMALIZE_NUMERIC_KEY", "false").lower() == "true"
        self._tag_source = os.environ.get("TAG_SOURCE", "")

    def _routing_key(self, msg: dict) -> str:
        shard = zlib.crc32(self._extract_bytes(msg)) % self.output_shards
        return str(shard)

    def process(self, data: dict) -> list:
        if self._normalize_numeric_key and self._key_field:
            value = str(data.get(self._key_field, "")).strip().lstrip("0")
            data[self._key_field] = value or "0"
        if self._tag_source:
            data["source"] = self._tag_source
        return [data]

    def on_eof(self, client_id=None):
        logger.info(f"EOF received for client_id={client_id}")
        return iter([])


if __name__ == "__main__":
    Splitter().run()

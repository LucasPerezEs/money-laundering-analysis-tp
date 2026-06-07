"""
WorkerBase: clase base para todos los workers.


MessageMiddlewareQueueRabbitMQ: para colas simples
ShardedExchangeConsumer: para consumir un shard de exchange
ShardedExchangeProducer: para publicar con sharding

Variables de entorno:
  RABBITMQ_HOST: host de RabbitMQ (default: rabbitmq)
  INPUT_QUEUE: cola de entrada (si consume de cola simple)
  INPUT_EXCHANGE: exchange de entrada (si consume de shard)
  CONSUMER_GROUP  : nombre logico de la etapa consumidora del exchange
  SHARD_ID        : id del shard de este worker
  N_UPSTREAM      : cantidad de EOFs a esperar
  OUTPUT_QUEUE    : cola de salida simple
  OUTPUT_EXCHANGE : exchange de salida con sharding
  OUTPUT_SHARDS   : cantidad de shards de salida (default 1)
  BATCH_SIZE      : filas por batch de salida (default 500)
"""
import logging
import os
import random
import signal
import time
import zlib
import hashlib
import json

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ, _connection_parameters
from common.middleware.middleware_sharded import ShardedExchangeConsumer, ShardedExchangeProducer
from common.middleware.middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
from common.message_protocol.internal import deserialize, serialize
from common.health.health_server import HealthCheckServer


logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RECONNECT_DELAY = 2
RECONNECT_MAX_DELAY = 30


def _wait_for_rabbitmq():
    while True:
        try:
            import pika
            conn = pika.BlockingConnection(_connection_parameters(RABBITMQ_HOST))
            conn.close()
            return
        except Exception:
            logger.warning(f"RabbitMQ no disponible, reintentando en {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)


class WorkerBase(HealthCheckServer):

    def __init__(self):
        self.input_queue     = os.environ.get("INPUT_QUEUE", "")
        self.input_exchange  = os.environ.get("INPUT_EXCHANGE", "")
        self.consumer_group  = os.environ.get("CONSUMER_GROUP", self.__class__.__name__)
        self.shard_id        = int(os.environ.get("SHARD_ID", "-1"))
        self.n_upstream      = int(os.environ.get("N_UPSTREAM", "1"))
        self.output_queue    = os.environ.get("OUTPUT_QUEUE", "")
        self.output_exchange = os.environ.get("OUTPUT_EXCHANGE", "")
        self.output_shards   = int(os.environ.get("OUTPUT_SHARDS", "1"))
        self.batch_size      = int(os.environ.get("BATCH_SIZE", "500"))
        self.total_clients   = int(os.environ.get("TOTAL_CLIENTS", "0"))

        self._buffer: dict = {}
        self._running = True

        self._processed_msgs_file = f"/tmp/processed_msgs_{self.consumer_group}_{self.shard_id}.txt"
        self._processed_msgs = set()
        if os.path.exists(self._processed_msgs_file):
            with open(self._processed_msgs_file, "r") as f:
                for line in f:
                    self._processed_msgs.add(line.strip())
        self._processed_file_handle = open(self._processed_msgs_file, "a")

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        _wait_for_rabbitmq()
        self.start_health_server()
        
        attempt = 0
        while True:
            try:
                _wait_for_rabbitmq()
                self._setup_connections()
                break
            except Exception as e:
                logger.warning(f"Fallo temporal de red/DNS iniciando conexiones: {e}. Reintentando...")
                self._reconnect_backoff(attempt)
                attempt += 1

    def _setup_connections(self):
        # Input
        if self.input_exchange and self.shard_id >= 0:
            self._consumer = ShardedExchangeConsumer(
                RABBITMQ_HOST, self.input_exchange, self.shard_id, self.consumer_group
            )
        elif self.input_queue:
            self._consumer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.input_queue)
        else:
            raise ValueError("Se requiere INPUT_QUEUE o INPUT_EXCHANGE + SHARD_ID")

        # cleanup_exc = os.environ.get("CLEANUP_EXCHANGE", "cleanup_exc")
        # self._cleanup_consumer = ShardedExchangeConsumer(
        #     RABBITMQ_HOST, cleanup_exc, self.shard_id, self.consumer_group
        # )

        # Output
        if self.output_exchange and self.output_shards >= 1:
            self._producer = ShardedExchangeProducer(RABBITMQ_HOST, self.output_exchange, self.output_shards)
        elif self.output_queue:
            self._producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.output_queue)
        else:
            self._producer = None

    def _close_resources(self):
        try:
            if hasattr(self, "_consumer") and self._consumer is not None:
                self._consumer.stop_consuming()
                self._consumer.close()
        except Exception:
            pass
        try:
            if hasattr(self, "_producer") and self._producer is not None:
                self._producer.close()
        except Exception:
            pass

    def _producer_is_open(self):
        if self._producer is None:
            return False
        conn = getattr(self._producer, "connection", None)
        if conn is None:
            return True
        return getattr(conn, "is_open", False)

    def _ensure_producer(self):
        if self._producer is None:
            return
        if not self._producer_is_open():
            try:
                self._producer.close()
            except Exception:
                pass
            if self.output_exchange and self.output_shards >= 1:
                self._producer = ShardedExchangeProducer(RABBITMQ_HOST, self.output_exchange, self.output_shards)
            elif self.output_queue:
                self._producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.output_queue)

    def _send_checkpoint(self, client_id, checkpoint_id):
        if self._producer is None:
            return
        checkpoint_body = serialize({
            "type": "checkpoint",
            "client_id": client_id,
            "checkpoint_id": checkpoint_id
        })
        self._ensure_producer()
        try:
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_eof_to_all(checkpoint_body)
            else:
                self._producer.send(checkpoint_body)
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            try:
                self._producer.close()
            except Exception:
                pass
            if self.output_exchange and self.output_shards >= 1:
                self._producer = ShardedExchangeProducer(RABBITMQ_HOST, self.output_exchange, self.output_shards)
            elif self.output_queue:
                self._producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.output_queue)
            
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_eof_to_all(checkpoint_body)
            else:
                self._producer.send(checkpoint_body)

    def _reconnect_backoff(self, attempt: int):
        delay = min(RECONNECT_DELAY * (2 ** attempt), RECONNECT_MAX_DELAY)
        logger.warning(f"Reintentando conexion en {delay}s...")
        time.sleep(delay)

    def _handle_sigterm(self, *_):
        logger.info("SIGTERM recibido -> cerrando")
        self._running = False
        try:
            self._consumer.stop_consuming()
        except Exception:
            pass

    # --- Para implementar en subclases -------------------------------------------

    def process(self, data: dict) -> list:
        raise NotImplementedError

    def on_eof(self, client_id=None) -> list:
        return []


    def _routing_key(self, msg: dict) -> str:
        if self.output_exchange and self.output_shards >= 1:
            routing_field = os.environ.get("ROUTING_FIELD")
            if routing_field and routing_field in msg:
                val = str(msg[routing_field]).encode()
                return str(zlib.crc32(val) % self.output_shards)
            else:
                # FIX: Enrutamiento determinístico basado en el contenido para tolerancia a fallos
                val = json.dumps(msg, sort_keys=True).encode()
                return str(zlib.crc32(val) % self.output_shards)
        return "__queue__"

    def _buffer_key(self, msg: dict) -> str:
        if self.output_exchange and self.output_shards >= 1:
            return self._routing_key(msg)
        if isinstance(msg, dict):
            client_id = msg.get("client_id")
            if client_id is not None:
                return f"client:{client_id}"
        return "__queue__"

    # --- Emisión con Buffer y flush --------------------------------------------------------

    def _emit(self, results: list):
        if not results or self._producer is None:
            return
        for msg in results:
            buf_key = self._buffer_key(msg)
            self._buffer.setdefault(buf_key, []).append(msg)
            if len(self._buffer[buf_key]) >= self.batch_size:
                self._flush_key(buf_key)

    def _flush_key(self, buf_key: str):
        rows = self._buffer.pop(buf_key, [])
        if not rows:
            return
        body = serialize({"rows": rows})
        self._ensure_producer()
        try:
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_to_shard(body, int(buf_key))
            else:
                self._producer.send(body)
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            try:
                self._producer.close()
            except Exception:
                pass
            if self.output_exchange and self.output_shards >= 1:
                self._producer = ShardedExchangeProducer(RABBITMQ_HOST, self.output_exchange, self.output_shards)
            elif self.output_queue:
                self._producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.output_queue)
            
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_to_shard(body, int(buf_key))
            else:
                self._producer.send(body)

    def _flush_all(self):
        for key in list(self._buffer.keys()):
            self._flush_key(key)

    def _send_eof(self, client_id=None):
        if self._producer is None:
            return
        eof_msg = {"type": "eof"}
        if client_id is not None:
            eof_msg["client_id"] = client_id
        eof_body = serialize(eof_msg)
        self._ensure_producer()
        try:
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_eof_to_all(eof_body)
            else:
                self._producer.send(eof_body)
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            try:
                self._producer.close()
            except Exception:
                pass
            if self.output_exchange and self.output_shards >= 1:
                self._producer = ShardedExchangeProducer(RABBITMQ_HOST, self.output_exchange, self.output_shards)
            elif self.output_queue:
                self._producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.output_queue)
            
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_eof_to_all(eof_body)
            else:
                self._producer.send(eof_body)
    # --- Loop principal ---------------------------------------------------------

    def run(self):
        logger.info(f"{self.__class__.__name__} iniciando")
        eof_count = [0]
        eof_per_client = {}
        done_clients = set()
        checkpoint_counts = {}
        
        # Diccionarios para guardar las funciones ACK hasta cumplir la condición
        checkpoint_acks = {}
        eof_acks_per_client = {}

        def on_message(body: bytes, ack, nack):
            try:
                msg_hash = hashlib.md5(body).hexdigest()
                if msg_hash in self._processed_msgs:
                    logger.warning(f"DUPLICADO IGNORADO ({msg_hash}) en {self.__class__.__name__}. Haciendo ack silencioso.")
                    ack()
                    return
                t0 = time.perf_counter()
                msg = deserialize(body)
                t_deser = time.perf_counter() - t0
                
                if msg.get("type") == "checkpoint":
                    client_id = msg.get("client_id")
                    checkpoint_id = msg.get("checkpoint_id")
                    chk_key = (client_id, checkpoint_id)
                    
                    checkpoint_acks.setdefault(chk_key, []).append(ack) # Diferir ACK
                    checkpoint_counts[chk_key] = checkpoint_counts.get(chk_key, 0) + 1
                    
                    if checkpoint_counts[chk_key] >= self.n_upstream:
                        self._flush_all()
                        self._send_checkpoint(client_id, checkpoint_id)
                        
                        # Confirmar todos los ACKs juntos
                        for pending_ack in checkpoint_acks[chk_key]:
                            pending_ack()
                        del checkpoint_acks[chk_key]
                        del checkpoint_counts[chk_key]
                    return
                
                elif msg.get("type") == "cleanup":
                    client_id = msg.get("client_id")
                    self._buffer.pop(f"client:{client_id}", None)
                    if hasattr(self, "_state"):
                        self._state.pop(client_id, None)
                    ack()
                    return
                    
                elif msg.get("type") == "eof":
                    t0_eof = time.perf_counter()
                    client_id = msg.get("client_id")
                    if client_id is None:
                        eof_count[0] += 1
                        logger.info(f"{self.__class__.__name__} EOF recibido ({eof_count[0]}/{self.n_upstream})")
                        ack()
                        if eof_count[0] >= self.n_upstream:
                            for result in self.on_eof(None):
                                self._emit([result])
                            self._flush_all()
                            self._send_eof()
                            self._consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado")
                        return

                    eof_acks_per_client.setdefault(client_id, []).append(ack) # Diferir ACK
                    eof_per_client[client_id] = eof_per_client.get(client_id, 0) + 1
                    logger.info(f"{self.__class__.__name__} EOF recibido para client_id={client_id} ({eof_per_client[client_id]}/{self.n_upstream})")
                    
                    if eof_per_client[client_id] >= self.n_upstream and client_id not in done_clients:
                        t1 = time.perf_counter()
                        for i, result in enumerate(self.on_eof(client_id)):
                            self._emit([result])
                            if i % 100 == 0:
                                self._consumer.process_events()
                                if self._producer:
                                    self._producer.process_events()
                        t_eof_logic = time.perf_counter() - t1
                        t2 = time.perf_counter()
                        self._flush_all()
                        self._send_eof(client_id)
                        t_eof_network = time.perf_counter() - t2
                        done_clients.add(client_id)
                        logger.info(f"EOF Total: {(time.perf_counter() - t0_eof):.4f}s | Lógica: {t_eof_logic:.4f}s | Red: {t_eof_network:.4f}s")
                        
                        # Confirmar todos los ACKs juntos
                        for pending_ack in eof_acks_per_client[client_id]:
                            pending_ack()
                        eof_acks_per_client[client_id] = []

                    if self.total_clients > 0 and len(done_clients) >= self.total_clients:
                        self._consumer.stop_consuming()
                        logger.info(f"{self.__class__.__name__} terminado")
                    return
                
                t_process = 0.0
                t_emit = 0.0
                for i, row in enumerate(msg.get("rows", [])):
                    t1 = time.perf_counter()
                    processed_data = self.process(row)
                    t2 = time.perf_counter()
                    self._emit(processed_data)
                    t3 = time.perf_counter()
                    t_process += (t2 - t1)
                    t_emit += (t3 - t2)

                    if i % 100 == 0:
                        self._consumer.process_events()
                        if self._producer:
                            self._producer.process_events()
                
                
                self._flush_all() 
                self._processed_msgs.add(msg_hash)
                self._processed_file_handle.write(msg_hash + "\n")
                self._processed_file_handle.flush()
                os.fsync(self._processed_file_handle.fileno()) # Fuerza la escritura a disco
                # ------------------------------------------------
                ack()
                logger.info(f"Tiempos -> Deserializar: {t_deser:.4f}s | Process: {t_process:.4f}s | Emit/Red: {t_emit:.4f}s")
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        attempt = 0
        while self._running:
            try:
                self._consumer.start_consuming(on_message)
                if self._running:
                    logger.warning("El consumo finalizo inesperadamente; reconectando")
                    self._close_resources()
                    _wait_for_rabbitmq()
                    self._reconnect_backoff(attempt)
                    self._setup_connections()
                    attempt += 1
                    continue
                break
            except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
                if not self._running:
                    break
                logger.error("Conexion perdida con RabbitMQ")
                self._close_resources()
                _wait_for_rabbitmq()
                self._reconnect_backoff(attempt)
                self._setup_connections()
                attempt += 1
            except Exception as e:
                logger.error(f"Error inesperado en {self.__class__.__name__}: {e}")
                break
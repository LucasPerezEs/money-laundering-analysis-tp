from common import message_protocol


class MessageHandler:

    def serialize_rows_message(self, client_id, rows):
        return message_protocol.internal.serialize(
            {"client_id": client_id, "rows": rows, "_worker_node_id": "gateway"}
        )

    def serialize_eof_message(self, client_id):
        return message_protocol.internal.serialize(
            {"type": "eof", "client_id": client_id, "_worker_node_id": "gateway"}
        )

    def serialize_checkpoint_message(self, client_id, checkpoint_id):
        return message_protocol.internal.serialize(
            {"type": "checkpoint", "client_id": client_id, "checkpoint_id": checkpoint_id, "_worker_node_id": "gateway"}
        )

    def deserialize_system_message(self, message):
        return message_protocol.internal.deserialize(message)

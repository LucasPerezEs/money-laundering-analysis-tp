import json
import os
from common.logger.base_node_logger import BaseNodeLogger

class PathsCreatorLogger(BaseNodeLogger):
    def __init__(self, base_filepath: str):
        super().__init__(base_filepath)
        self.creator_state_filepath = f"{base_filepath}_creator_state.json"

    def _serialize_edges(self, edges_dict: dict) -> dict:
        """Convierte {client: {tupla: set(tuplas)}} a {client: {str: list(list)}}"""
        serializable = {}
        for client_id, interm_dict in edges_dict.items():
            serializable[client_id] = {
                json.dumps(interm_node): [list(node) for node in target_nodes_set]
                for interm_node, target_nodes_set in interm_dict.items()
            }
        return serializable

    def _deserialize_edges(self, raw_dict: dict) -> dict:
        """Convierte {client: {str: list(list)}} de vuelta a {client: {tupla: set(tuplas)}}"""
        recovered = {}
        for client_id, interm_dict in raw_dict.items():
            recovered[client_id] = {
                tuple(json.loads(interm_node_str)): set(tuple(node_list) for node_list in target_nodes_list)
                for interm_node_str, target_nodes_list in interm_dict.items()
            }
        return recovered

    def save_creator_state(self, incoming: dict, outgoing: dict):
        state = {
            "incoming": self._serialize_edges(incoming),
            "outgoing": self._serialize_edges(outgoing)
        }
        tmp_path = self.creator_state_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(state, f)
        os.replace(tmp_path, self.creator_state_filepath)

    def recover_creator_state(self) -> tuple:
        """Retorna (incoming_edges, outgoing_edges). Si no hay estado, retorna dos dicts vacíos."""
        if not os.path.exists(self.creator_state_filepath):
            return {}, {}
        try:
            with open(self.creator_state_filepath, "r") as f:
                raw_state = json.load(f)
            
            incoming = self._deserialize_edges(raw_state.get("incoming", {}))
            outgoing = self._deserialize_edges(raw_state.get("outgoing", {}))
            return incoming, outgoing
        except (json.JSONDecodeError, IOError):
            return {}, {}
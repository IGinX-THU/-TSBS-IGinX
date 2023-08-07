KEY_PATH = "key"
ENTITY_PATH = "truck"
FIELD_PATH = "name"
VALUE_PATH = "value"

TYPE_DOUBLE = "DOUBLE"
TYPE_LONG = "LONG"
TYPE_BINARY = "BINARY"
TYPE_STRING = "STRING"

class UDFTranspositionByTruck:
    def __init__(self):
        pass

    @staticmethod
    def _spilt_path(path):
        path_nodes = path.split(".")
        assert len(path_nodes)>=3 , f"too less path nodes: {path_nodes}"
        return (path_nodes[1].encode(),path_nodes[-1].encode())

    def transform(self, rows):
        paths = [UDFTranspositionByTruck._spilt_path(path) for path in rows[0]]

        return [
            [ENTITY_PATH, FIELD_PATH, VALUE_PATH],
            [TYPE_BINARY, TYPE_BINARY, TYPE_DOUBLE],
        ] + [
            [truck, name, value]
            for row in rows[2:]
            for (truck, name), value in zip(paths, row)
        ]

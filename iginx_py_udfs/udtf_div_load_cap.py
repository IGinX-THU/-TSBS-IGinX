from collections import defaultdict

TYPE_DOUBLE = "DOUBLE"
LOAD = "current_load"
CAP = "load_capacity"

class UDFDivLoadCap:
    def __init__(self):
        pass

    @staticmethod
    def _clean_path(path: str):
        if path[-1] == ")":
            return path[path.find("(") + 1 : -1]
        return path

    @staticmethod
    def _spilit_path(path: str):
        return path.rsplit(".", 1)

    @staticmethod
    def _div_load_cap(trucks: dict[dict]):
        for truckname, truck in trucks.items():
            if LOAD not in truck or CAP not in truck:
                continue
            yield truckname + ".avgload", TYPE_DOUBLE, truck[LOAD] / truck[CAP]

    def transform(self, data):
        paths = (
            UDFDivLoadCap._spilit_path(UDFDivLoadCap._clean_path(path))
            for path in data[0]
        )

        trucks = defaultdict(dict)
        for (truckname, field), value in zip(paths, data[2]):
            trucks[truckname][field] = value

        return list(map(list, zip(*UDFDivLoadCap._div_load_cap(trucks))))

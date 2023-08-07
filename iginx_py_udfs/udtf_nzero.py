TYPE_DOUBLE = "DOUBLE"


class UDFNZero:
    def __init__(self):
        pass

    def transform(self, data):
        return [
            [path + "nz" for path in data[0]],
            [TYPE_DOUBLE] * len(data[1]),
            [float(num != 0) if num is not None else None for num in data[2]],
        ]
    


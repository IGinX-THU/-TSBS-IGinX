TYPE_DOUBLE = "DOUBLE"


class UDFDiv144:
    def __init__(self):
        pass

    def transform(self, data):
        return [
            ["div_144(" + path + ")" for path in data[0]],
            [TYPE_DOUBLE] * len(data[1]),
            [float(num) / 144 for num in data[2]],
        ]

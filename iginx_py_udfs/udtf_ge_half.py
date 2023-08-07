import math

TYPE_DOUBLE = "DOUBLE"

class UDFGeHalf:
    def __init__(self):
        pass

    def transform(self, data):
        return [
            data[0],
            data[1],
            [float(num>=0.5) for num in data[2]],
        ]

class UDFLOne:
    def __init__(self):
        pass

    def transform(self, data):
        return [
            ["l_one(" + path + ")" for path in data[0]],
            ["DOUBLE"] * len(data[1]),
            [1.0 if num <1 else 0.0 for num in data[2]],
        ]

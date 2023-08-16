class UDFCountUp:
    def __init__(self):
        pass

    def transform(self, data):
        count = [0.0] * len(data[0])
        for lastrow, row in zip(data[2:], data[3:]):
            for i, (lastv, currv) in enumerate(zip(lastrow, row)):
                if lastv is not None and currv is not None:
                    if currv > lastv:
                        count[i] += 1

        return [
            ["count_up(" + path + ")" for path in data[0]],
            ["DOUBLE"] * len(data[1]),
            count,
        ]
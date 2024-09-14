class UDFifbreak:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)

        countRow = []
        rows = data[2:]
        for row in list(zip(*rows))[1:]:
            count = 0
            count1 = 0
            for num in row:
                if num is not None:
                    count += 1
                    if num == 0:
                        count1 += 1
            countRow.append((count1 / count) >= 0.5)
        res.append(countRow)
        return res

    def buildHeader(self, data):
        colNames = []
        colTypes = []
        for name in data[0][1:]:
            colNames.append("ifbreak(" + name + ")")
            colTypes.append("BOOLEAN")
        return [colNames, colTypes]
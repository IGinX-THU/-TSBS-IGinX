def generate_windows(timestamps, window_size_ns):
    min_timestamp = min(timestamps)
    
    windows = []

    for ts in timestamps:
        offset_from_min = ts - min_timestamp
        window_start = min_timestamp + (offset_from_min // window_size_ns) * window_size_ns
        windows.append(window_start)

    return windows


class UDFTimebucket10m:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        windowRow = generate_windows([row[1] for row in data[2:]], 600_000_000_000)
        for row_index in range(len(data) - 2):
            data[row_index+2][1] = windowRow[row_index]
        res.extend(data[2:])
        return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0]:
            if name != "key":
                colNames.append("timebucket10m(" + name + ")")
            else:
                colNames.append(name)
        return [colNames, data[1]]

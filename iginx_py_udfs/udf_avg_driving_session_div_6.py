import math

def check_driving(num):
    return math.floor(num / 10) != 0

def driving_session(seq):
    first = None
    for i, is_driving in enumerate(seq):
        if first is None:
            if is_driving:
                first = i
            continue
        if not is_driving:
            yield i - first
            first = None

def mean_session(seq):
    seqlist = list(seq)
    if len(seqlist) == 0:
        return 0
    return sum(seqlist) / len(seqlist)


class UDFAvgDrivingSessionDiv6:
    def __init__(self):
        pass

    def transform(self, data):
        return [
            ["avg_session(" + path + ")" for path in data[0]],
            ["DOUBLE"] * len(data[0]),
            [
                mean_session(driving_session(map(check_driving, seq))) / 6
                for seq in zip(*data[2:])
            ],
        ]

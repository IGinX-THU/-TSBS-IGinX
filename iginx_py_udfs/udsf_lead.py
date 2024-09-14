import pandas as pd

class UDFlead:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        df = pd.DataFrame(data[2:], columns=['key','tagid', 'ten', 'broken_down'])
        
        df['next_broken_down'] = df.groupby('tagid')['broken_down'].shift(-1)
        df_filtered = df.dropna(subset=['next_broken_down'])

        result = df_filtered[['key', 'tagid', 'ten', 'broken_down', 'next_broken_down']].values.tolist()
        for row in result:
            row[0] = int(row[0])  
            row[1] = int(row[1])
            row[2] = int(row[2])
        res.extend(result)
        return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0]:
            if name != "key":
                colNames.append("lead(" + name + ")")
            else:
                colNames.append(name)
        colNames.append("lead(next_broken_down)")
        colTypes = []
        for type in data[1]:
            colTypes.append(type)
        colTypes.append("BOOLEAN")
        return [colNames, colTypes]

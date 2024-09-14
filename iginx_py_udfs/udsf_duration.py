import pandas as pd

class UDFDuration:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        df = pd.DataFrame(data[2:], columns=['key','tagid', 'ten', 'driving'])
        
        df['prev_driving'] = df.groupby('tagid')['driving'].shift(1)
        df_filtered = df.dropna(subset=['prev_driving'])
        
        df_filtered = df_filtered[(df_filtered['driving'] >  5 ) != (df_filtered['prev_driving'] > 5)]
        
        df_filtered['stop'] = df_filtered.groupby('tagid')['ten'].shift(-1)
        
        result = df_filtered.dropna(subset=['stop'])
        result = result[['key', 'tagid', 'ten', 'driving', 'stop']].values.tolist()
        for row in result:
            row[0] = int(row[0])  
            row[1] = int(row[1])
            row[2] = int(row[2])
            row[4] = int(row[4])
        res.extend(result)
        return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0]:
            if name != "key":
                colNames.append("startstop(" + name + ")")
            else:
                colNames.append(name)
        colNames.append("startstop(stop)")
        colTypes = []
        for type in data[1]:
            colTypes.append(type)
        colTypes.append("LONG")
        return [colNames, colTypes]

import pandas as pd
import json

json_file = 'INPUT.json'
messages_df = pd.read_json(json_file)

messages_df['diffe'] = 0
messages_df = messages_df[['user', 'ts', 'diffe', 'text']].sort_values('ts')
groupbyuser = messages_df.groupby('user')

for key, item in groupbyuser:
    subgroup = groupbyuser.get_group(key)
    subgroup = subgroup.sort_values('ts')
    subgroup['diffe'] = subgroup['ts'].diff()
    subgroup['diffe'].fillna(0, inplace=True)
    subgroup['grupo']=0
    first = True
    grupo = 0
    lastTime=0
    for key2, item2 in subgroup.iterrows():
        if first:
            subgroup.loc[key2, ['grupo']] = 0
            first=False
        elif (item2['diffe']-lastTime)<=120:
            subgroup.loc[key2,['grupo']] = grupo
        else:
            grupo = grupo + 1
            subgroup.loc[key2, ['grupo']] = grupo
        lastTime = item2['diffe']

    def column(matrix, i):
        return [row[i] for row in matrix]

    print(subgroup.to_string())

    file = (subgroup[0:1]['user'].values)[0] + '.json'
    chaves = column(subgroup.groupby('grupo')['ts'].apply(list),0)  #set
    mensagens = subgroup.groupby('grupo')['text'].apply(list)  # set

    final={}
    for index in range (len(chaves)):
        final[chaves[index]] = mensagens[index]

    with open(file, 'w') as fp:
        json.dump(final, fp)
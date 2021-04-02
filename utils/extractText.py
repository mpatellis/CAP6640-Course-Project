import pandas as pd

def extract(path):
    print(path)
    df = pd.read_csv(path, compression='gzip', index_col='id')
    text = df['full_text']
    return text.tolist()

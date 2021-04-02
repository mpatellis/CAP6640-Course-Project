import argparse
import os, sys
import pandas as pd

def get_args():
    cli_parse = argparse.ArgumentParser(description='Point at a directory of tweets. Will create two csvs. One with biden references and one with trump references')

    cli_parse.add_argument('Path', type=str, help='Path to the input directory')

    return cli_parse.parse_args()

def process_files(files, path):
    n = len(files)
    biden_words = ['joe', 'biden']
    trump_words = ['donald', 'trump']

    biden = pd.DataFrame()
    trump = pd.DataFrame()

    print('Total of %d files to be processed' % n)

    count = 0
    for f in files:
        if path[-1] == '/':
            f_path = path + f
        else:
            f_path = path + '/' + f

        try:
            temp = pd.read_csv(f_path)
        except:
            print('Error: ', sys.exc_info()[0])
            print(path)
            continue

        b_biden = []
        for b in biden_words:
            b_biden.append(temp['full_text'].str.contains(b, case=False))
        
        temp_biden = temp[b_biden[0] | b_biden[1]]

        biden = pd.concat([biden, temp_biden], ignore_index=True)

        b_trump = []
        for t in trump_words:
            b_trump.append(temp['full_text'].str.contains(t, case=False))
        
        temp_trump = temp[b_trump[0] | b_trump[1]]

        trump = pd.concat([trump, temp_trump], ignore_index=True)

        count = count + 1
        print('%d of %d' % (count, n))
    
    seps = [i for i, c in enumerate(path) if c == '/']

    fn = ''
    if path[-1] == '/':
        fn = path[seps[-2]+1:-1]
    else:
        fn = path[seps[-1]+1:]
    
    biden.to_csv(fn + '-biden.csv.gz', compression='gzip')
    trump.to_csv(fn + '-trump.csv.gz', compression='gzip')




def main():
    args = get_args()

    path = args.Path

    files = []
    for file in os.listdir(path):
        if file.endswith('.csv.gz'):
            files.append(file)

    process_files(files, path)


if __name__ == '__main__':
    main()
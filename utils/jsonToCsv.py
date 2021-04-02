import argparse
import gzip
import os, sys
import pandas as pd

# Sets up the arguments for the file. Use -h for info
def get_args():
    cli_parse = argparse.ArgumentParser(description='Cleans and converts jsonl tweet data to csv. Will move back one directory from provided path and create out directory')

    cli_parse.add_argument('Path', type=str, help='Path to either the input file or directory')
    cli_parse.add_argument('-o', action='store', help='Path for output directory. If None then output will be "Path/../processed_tweets"')
    cli_parse.add_argument('-rt', action='store_true', help='If included will remove retweets')
    cli_parse.add_argument('-s', action='store_true', help='Include if path is a single file. Will place the resulting csv.gz wherever this script is run from.')

    return cli_parse.parse_args()

# Drops the unneeded columns. To add/remove columns change the saved_cols list
def dropCols(df):
    saved_cols = ['id', 'full_text', 'geo', 'coordinates', 'place', 'retweet_count', 'favorite_count']
    remove_cols = [col for col in df.columns if col not in saved_cols]
    df = df.drop(columns=remove_cols)
    return df

# Removes actual retweets. Checks for retweeted_status in the dataframe
def removeRT(df):
    try:
        df = df[(df['retweeted_status'].isna())]
    except KeyError:
        print('No retweeted_status')
    
    return df

# Processes a single file. Used for general directory case and when -s is used
def process_single_file(path, clean_rt, out_path):
    filename = path.split('/')[-1][:-9]

    try:
        df = pd.read_json(path, lines=True, compression='gzip')
    except:
        print('Error: ', sys.exc_info()[0])
        print(path)
        return

    if clean_rt:
        df = removeRT(df)
        filename = filename + '-noRT'

    df = dropCols(df)

    if out_path == None:
        filename = filename + '.csv.gz'
    else:
        filename = out_path + '/' + filename + '.csv.gz'
    
    df.to_csv(path_or_buf=filename, compression='gzip')

# Takes a list of file names and passes them to process_single_file for actual processing
def process_files(files, path, out_path, clean_rt):
    n = len(files)
    print('Total of %d files to be processed' % n)
    count = 0
    for f in files:
        f_path = path + '/' + f
        process_single_file(f_path, clean_rt, out_path)
        count = count + 1
        print('%d of %d' % (count, n))

def main():
    args = get_args()

    path = args.Path
    clean_rt = args.rt
    single_file = args.s
    out_path = args.o

    if single_file:
        process_single_file(path, clean_rt, None)
        return
    
    if out_path == None:
        out_path = os.path.join(path, '../processed_tweets')

    try:
        os.makedirs(out_path, exist_ok=True)
    except OSError as error:
        print(error)
        exit()

    files = []
    for file in os.listdir(path):
        if file.endswith('.jsonl.gz'):
            files.append(file)

    process_files(files, path, out_path, clean_rt)

if __name__ == '__main__':
    main()

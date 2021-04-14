import argparse
import gzip
import os, sys
import threading
import pandas as pd

import concurrent.futures

# Sets up the arguments for the file. Use -h for info
def get_args():
    cli_parse = argparse.ArgumentParser(description='Cleans and converts jsonl tweet data to parquet. Also filters based on language and only includes tweets with joe, biden, donald, trump. Always removes retweets. Outputs single in directory called')

    cli_parse.add_argument('Path', type=str, help='Path to either the input file or directory')
    cli_parse.add_argument('Output_Name', type=str, help='Name of outputted file')
    cli_parse.add_argument('-s', action='store_true', help='Include if path is a single file. Will place the resulting csv.gz wherever this script is run from.')
    cli_parse.add_argument('Num_Threads', type=int, help='Number of threads for reading')

    return cli_parse.parse_args()

# Drops the unneeded columns. To add/remove columns change the saved_cols list
def dropCols(df):
    saved_cols = ['id', 'full_text', 'retweet_count', 'favorite_count', 'created_at']
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
def process_single_file(path):
    filename = path.split('/')[-1][:-9]
    try:
        df = pd.read_json(path, lines=True, compression='gzip')
    except:
        print('Error: ', sys.exc_info()[0])
        print(path)
        return
        
    df = removeRT(df)
    df = removeNotEnglish(df)

    df = dropCols(df)

    df['id'].astype(str)

    words = ['joe', 'biden', 'donald', 'trump']
    b_words = []
    for w in words:
        b_words.append(df['full_text'].str.contains(w, case=False))
    
    df = df[b_words[0] | b_words[1] | b_words[2] | b_words[3]]

    return df

def removeNotEnglish(df):
    return df[df['lang'] == 'en']

# Takes a list of file names and passes them to process_single_file for actual processing
def process_files(files, path):
    n = len(files)
    print('Total of %d files to be processed' % n)
    count = 0
    df = pd.DataFrame()

    for f in files:
        f_path = path + '/' + f
        df = pd.concat([df, process_single_file(f_path)], ignore_index=True)

        count = count + 1
        if count > n/2:
            print('Halfway')
    
    print('Complete')
    return df

def main():
    args = get_args()

    path = args.Path
    single_file = args.s
    output_name = args.Output_Name
    num_threads = args.Num_Threads

    if single_file:
        process_single_file(path, clean_rt, None)
        return

    files = []
    for file in os.listdir(path):
        if file.endswith('.jsonl.gz'):
            files.append(file)

    num_files_per = len(files) // num_threads
    num_leftover = len(files) % num_threads
    list_num_files = []

    for i in range(num_threads):
        list_num_files.append(num_files_per)
        if num_leftover != 0:
            list_num_files[-1] = list_num_files[-1] + 1
            num_leftover = num_leftover - 1
    
    file_list = []
    start_index = 0
    for i in range(len(list_num_files)):
        end_index = start_index + list_num_files[i]
        file_list.append(files[start_index:end_index])
        start_index = end_index

    df_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_files, f, path) for f in file_list]
        df_list = [f.result() for f in futures]

    df = pd.concat(df_list, ignore_index=True)

    df.to_parquet(output_name, compression='snappy')

if __name__ == '__main__':
    main()

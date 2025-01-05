import os

def get_files(directory, ext):
    # A function that returns a list of files in the specified directory
    files = [i for i in os.listdir(directory) if ext in i]
    return files

def get_directories(path):
    # A function that returns a list of directories
    directories = []
    for root, dirs, files in os.walk(path):
        for name in dirs:
            if name[0] != '.':
                directories.append(name)
    return directories

def get_drop_columns(col_names, columns_to_keep):
    # A function that creates a list of columns to drop from the inventory

    columns_to_drop = [] # an empty list to store column names to drop
    for i in col_names:
        if i not in columns_to_keep:
                columns_to_drop.append(i)
    return columns_to_drop

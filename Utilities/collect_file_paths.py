import os

def collect_file_paths(directory, extension):
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.' + extension):
                file_paths.append(os.path.join(root, file))
    return file_paths

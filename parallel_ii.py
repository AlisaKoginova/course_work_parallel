from multiprocessing import Process
import os
from time import time

class MyProcess(Process):

    def __init__(self, directory, files):
        Process.__init__(self)
        self.directory = directory
        self.files = files
        self.process_storage = {}


    def open_files(self):
        '''
        read all files in a current process's chunk
        split text in every file with ' '
        call normalize_word
        call inverted_index
        '''
        for file in self.files:
            with open(self.directory + file) as file_handler:
                for text in file_handler:
                    text = text.split(' ')
                    text = [self.normalize_word(x) for x in text]
                    self.inverted_index(text, file)


    def normalize_word(self, word):
        '''
        word -> lower()
        remove signs
        '''
        signs = [',', '!', '/', '?', '.', '(', ')', "'", '"', '<', '>', ':', ';', '_']
        word = word.lower()
        if word:
            for sign in signs:
                if sign in word:
                    word = word.replace(sign, '')
        return word


    def inverted_index(self, line, docID):
        '''
        add to index_dictionary: keys - 'word', values - (docID, position in file)
        '''
        position = 0
        for word in line:
            if word not in self.process_storage.keys():
                self.process_storage[word] = [(docID, position)]
            else:
                self.process_storage[word] += ([(docID, position)])
            position += 1


    def run(self):
        self.open_files()
        return self.process_storage


def divide_data(files, num_of_processes):
    '''
    divide data into similar chunks
    '''
    chunk_size = int(len(files) / num_of_processes)
    division_list = [x * chunk_size for x in range(num_of_processes)]
    division_list.append(None)

    return division_list


def merge_dictionaries(local_dictionaries):
    '''
    merge local dictionaries returned from processes into one
    '''
    root = local_dictionaries[0]
    for local_dict in local_dictionaries[1:]:
        for key in local_dict.keys():
            if key in root.keys():
                root[key] += local_dict[key]
            else:
                root[key] = local_dict[key]

    return root


if __name__ == '__main__':

    directory = 'C:/Users/Alisa/Desktop/test/'
    files = os.listdir(directory) # list of file names ['8251_3.txt', '8251_7.txt', ...]
    num_of_processes = 2

    start_time = time()
    division_list = divide_data(files, num_of_processes)
    local_dictionaries = []
    processes = []

    for i in range(num_of_processes):
        p = MyProcess(directory, files[division_list[i]:division_list[i+1]])
        processes.append(p)

    local_dictionaries += [x.run() for x in processes]
    inverted_index = merge_dictionaries(local_dictionaries)

    print(f'Parallel result: {time() - start_time}')
    with open('inverted_index_parallel.txt', 'a') as file:
        file.write(str(inverted_index))

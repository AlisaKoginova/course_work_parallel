from multiprocessing import Process, Manager
import os
from time import time

class InvertedIndex:

    def __init__(self, directory, num_of_processes):
        m = Manager()
        self.index_dictionary = m.Queue()
        self.directory = directory
        self.num_of_processes = num_of_processes
        self.files = os.listdir(directory)


    def normalize_word(self, word):
        '''
        word -> lower()
        remove signs
        '''

        signs = [',', '!', '/', '?', '.', '(', ')', "'", '"', '<', '>']
        word = word.lower()
        if word:
            for sign in signs:
                if sign in word:
                    word = word.replace(sign, '')
        return word


    def divide_data(self):
        N = int(len(self.files) / self.num_of_processes)
        result = [x * N for x in range(self.num_of_processes)]
        result.append(None)

        return result


    def run(self):
        processes = []
        chunks = self.divide_data()

        for i in range(self.num_of_processes):
            p = Process(target=self.open_files, args=(self.files[chunks[i]:chunks[i+1]],))
            processes.append(p)

        [x.start() for x in processes]
        [x.join() for x in processes]


    def open_files(self, files_block):
        for file in files_block:
            with open(self.directory + file) as file_handler:
                for text in file_handler:
                    text = text.split(' ')
                    text = [self.normalize_word(x) for x in text]
                    self.inverted_index(text, file)


    def inverted_index(self, line, docID):
        '''
        add to index_dictionary: keys - 'word', values - (docID, position in file)
        '''
        position = 0
        local_dict = {}
        for word in line:
            if word not in local_dict.keys():
                local_dict[word] = [(docID, position)]
            else:
                local_dict[word] += ([(docID, position)])
            position += 1

        self.index_dictionary.put(local_dict, timeout=None)


    def save_index_dictionary(self):
        '''
        save index_dictionary as txt file
        '''
        with open('inverted_index_parallel.txt', 'a') as file:
            while not self.index_dictionary.empty():
                file.write(str(self.index_dictionary.get(timeout=None)))


    def __call__(self):
        self.run()
        self.save_index_dictionary()


if __name__ == '__main__':
    ii = InvertedIndex(directory='C:/Users/Alisa/Desktop/test/', num_of_processes=2)
    start_time = time()
    ii()
    print(time() - start_time)

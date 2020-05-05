
from multiprocessing import Manager, Pool
import os
from time import time

class InvertedIndex:

    def __init__(self, directory, num_of_processes):
        manager = Manager()
        self.index_dictionary = manager.dict()
        self.directory = directory
        self.num_of_processes = num_of_processes
        self.files = os.listdir(directory)


    def normalize_word(self, word):
        '''
        word -> lower()
        remove signs
        '''

        signs = [',', '!', '/', '?', '.', '(', ')', "'", '"']
        word = word.lower()
        if word:
            for sign in signs:
                if sign in word:
                    word = word.replace(sign, '')
        return word


    def test(self):
        with Pool(processes=self.num_of_processes) as pool:
            pool.map(self.open_files, self.files, chunksize=10)


    def open_files(self, files_block):
        with open(self.directory + files_block) as file_handler:
            for text in file_handler:
                text = text.split(' ')
                text = [self.normalize_word(x) for x in text]
                self.inverted_index(text, files_block)


    def inverted_index(self, line, docID):
        '''
        add to index_dictionary: keys - 'word', values - (docID, position in file)
        '''
        position = 0
        for word in line:
            if word not in self.index_dictionary.keys():
                self.index_dictionary[word] = [(docID, position)]
            else:
                self.index_dictionary[word] += [(docID, position)]
            position += 1


    def save_index_dictionary(self):
        '''
        save index_dictionary as txt file
        '''

        with open('inverted_index_parallel.txt', 'a') as file:
            file.write(str(self.index_dictionary))


    def __call__(self):
        self.test()


if __name__ == '__main__':
    ii = InvertedIndex(directory='C:/Users/Alisa/Desktop/test2/', num_of_processes=4)
    start_time = time()
    ii()
    print(time() - start_time)








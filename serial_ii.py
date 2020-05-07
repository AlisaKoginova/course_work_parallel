import os
from time import time

class InvertedIndex:

    def __init__(self, directory):
        self.directory = directory
        self.files = os.listdir(directory)
        self.index_dictionary = {}


    def open_files(self):
        '''
        read all files in directory
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
            if word not in self.index_dictionary.keys():
                self.index_dictionary[word] = [(docID, position)]
            else:
                self.index_dictionary[word] += ([(docID, position)])
            position += 1


    def save_index_dictionary(self):
        '''
        save index_dictionary as txt file
        '''
        with open('inverted_index_serial.txt', 'a') as file:
            file.write(str(self.index_dictionary))


    def __call__(self):
        self.open_files()
        self.save_index_dictionary()


if __name__ == '__main__':
    directory = 'C:/Users/Alisa/Desktop/test/'
    start_time = time()
    ii = InvertedIndex(directory)
    ii()
    print(f'Serial result: {time() - start_time}')

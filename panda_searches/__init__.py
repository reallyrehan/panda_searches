from collections import defaultdict
import sys
import pandas as pd
from tqdm.auto import tqdm

@pd.api.extensions.register_dataframe_accessor("ps")
class PandaSearches:
    def __init__(self,pandas_obj = None):
        self.tqdm_flag=None
        self.df = pandas_obj
        self.index_dictionary = {}
        self.bytes_dictionary = None
        
#         if index:
#             self.__indexing()
#             self.__estimating_memory()

    def __estimating_memory(self):
        
        self.bytes_dictionary = {}

        tot_size = 0
        for col in self.index_dictionary.keys():
            mem_size = sum([sys.getsizeof(i) for i in self.df[col].unique()]) + len(self.df)*28
            self.bytes_dictionary[col] = mem_size
            
            tot_size += mem_size
            
        self.bytes_dictionary['total'] = tot_size

        
#         return self.bytes_dictionary


    def list_index(self):
        return self.index_dictionary.keys()
    
    def estimating_memory(self):
        
        if not self.bytes_dictionary:
            self.__estimating_memory()
            
        return self.bytes_dictionary
                
    def indexing(self,*index_columns):        
        index_columns = list(index_columns)
        
        for col in index_columns:
            try:
                self.__indexing(col)
            except KeyError as e:
                print(f"Column {e} does not exist.")

        self.__estimating_memory()
        
    def delete_index(self,*index_columns):
        index_columns = list(index_columns)
        
        for col in index_columns:
            try:
                del self.index_dictionary[col]
            except KeyError as e:
                print(f"Column {e} not indexed")
        
        self.__estimating_memory()
        
    def progress_bar(self,iterator,total):
        try:
            return tqdm(iterator,total=total)
        except:
            if not self.tqdm_flag:
                self.tqdm_flag = True
                print("Hint: Install and import tqdm for progress bar --> from tqdm.auto import tqdm")
            return iterator
    
    def __indexing(self,col):
        print(f"Indexing {col}")
        
        col_iterator = zip(self.df.index,self.df[col])
        
        self.index_dictionary[col] = defaultdict(list)

        for index,val in self.progress_bar(col_iterator,len(self.df)):
            self.index_dictionary[col][val].append(index)
        print("============")

    def __fetch(self,tup):
        
        col,key_initial = tup
        
        if callable(key_initial):
            func = key_initial
            key = []
            
            key = list(self.index_dictionary[col].keys())
            
            key = [i for i in key if func(i)]    
        else:
            key = key_initial
            
                    
        if isinstance(key,list):
            
            temp_ls = []
            
            for k in key:
                temp_ls.extend(self.index_dictionary[col][k])
            return temp_ls
        
        else:
            return self.index_dictionary[col][key]
        
    def __search_recursive(self,equation):
        and_gate,or_gate = False,False


        if isinstance(equation,list):
            for tup in equation:

                if tup in ['and','or']:
                    if tup =='and':
                        and_gate = True
                    if tup == 'or':
                        or_gate = True
                    continue

                current_set = self.__search_recursive(tup)

                if and_gate:
                    and_gate = False
                    current_set = current_set.intersection(previous_set)

                if or_gate:
                    or_gate = False
                    current_set = current_set.union(previous_set)

                previous_set = current_set

            return current_set
        else:
            return set(self.__fetch(equation))
        
    def search(self,equation = None,index_only=False):
        try:
            get_results = list(self.__search_recursive(equation))
            if index_only:
                return get_results
        except KeyError as e:
            if e.args[0] in self.df.columns:
                print(f"Column {e} is not indexed. Please run indexing() with provided column names.")
            else:
                print(f"Column {e} does not exist.")
        
        else:
            return self.df.iloc[get_results]
    
    def __getitem__(self, equation):
        
        return self.search(equation)
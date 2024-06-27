''' Streaming music sources and libraries '''

import os
from glob import glob

from pandas import DataFrame, read_csv, concat

from common.structure import CRITICS_FOLDER 
from music.dsp import Service

class Critic(Service):
    name = 'Best Albums Rankings'

    folder = CRITICS_FOLDER
    extension = 'csv'
    
    def __init__(self):
        super().__init__()
        
    def get_critic_files(self, excludes=DataFrame()):
        existing = excludes.apply(lambda x: self.make_file_name(x['critic_name'], x['list_year']),
                                  axis=1).values if not excludes.empty else []
        critic_files = [critic_name_year for f in glob(os.path.join(self.folder, f'*.{self.extension}')) \
                        if (critic_name_year:=os.path.splitext(os.path.basename(f))[0]) not in existing]
        return critic_files

    def get_critic_lists(self, excludes=None):
        critic_files = self.get_critic_files(excludes)
             
        if len(critic_files):
            print('getting critics picks')
            critic_lists = []
            total_files = len(critic_files)
            for i, critic_file in enumerate(critic_files):
                critic_name, list_year = self.get_name_and_year(critic_file)
                self.show_progress(i, total_files, message=f'{critic_name} {list_year}')
                critic_lists.append(self.get_critic_list(critic_file))
            self.show_progress()
            lists_df = concat(critic_lists)
        else:
            lists_df = DataFrame()
            
        return lists_df
    
    def get_critic_list(self, file_name):
        file_path = f'{self.folder}/{file_name}.{self.extension}'
        critic_name, list_year = self.get_name_and_year(file_name)
        critic_list = read_csv(file_path)
        critic_list.loc[:, 'artist_names'] = critic_list['artist_names'].apply(lambda x: x.split('; '))
        critic_list.loc[:, ['critic_name', 'list_year']] = [critic_name, list_year]
        return critic_list
    
    def get_name_and_year(self, file_name):
        critic_name = file_name[:-5].replace('_', ' ').title()
        list_year = int(file_name[-4:])
        return critic_name, list_year
    
    def make_file_name(self, critic_name, list_year):
        file_name = critic_name.lower().replace(' ', '_') + f'_{list_year}'
        return file_name
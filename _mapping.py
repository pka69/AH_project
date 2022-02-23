import pandas as pd
from ._common import ProcessingMixIn, MappingData
from .df_manipulation import import_data, change_columns_type, drop_duplicates


class AccMapping(MappingData, ProcessingMixIn):
    def __init__(self,def_dict, dir):
        super().__init__(name='Account mapping', msg=['map_GCAD failure: ', 'GCAD mapping completed'])
        self.dir = dir
        self.def_dict = def_dict
        self.mapping_columns = ['ACC_ID', 'GCAD_ID', 'ACC_GROUP', 'TYPE', 
                        'ACCOUNT_NAME', 'ACCOUNT_NAME_LONG']
        
    def initial_process(self):
        processes = [
            [import_data, True, {'def_dict': self.def_dict, 'df': self.df, 'source_dir': self.dir}],
            [change_columns_type, True, {'df': self.df, 'types': self.def_dict.get('columns_types',None)}],
            [drop_duplicates, True, {'df': self.df, 'columns': ['ACC_ID', 'GCAD_ID']}],
        ]
        self.wraper(self._run_processes)(processes, 'import acc_mapping')

class EntityMapping(MappingData, ProcessingMixIn):
    def __init__(self,def_dict, dir):
        super().__init__(name="Entity mapping", msg=['Entity mapping failure: ', 'Entity mapping completed'])
        self.def_dict = def_dict
        self.dir = dir
        self.mapping_columns = [ 'parent / base', 'ID', 'description',  # 'MATCHED ENTITY', 'ID within ARA', 'No','ID name', 
                     'H1', 'EPM Reporting Entity', 'Levels', 'Base / parent', # 'Group_Currency', 
                    'L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'L8', 'EPM_ENTITY']

    def initial_process(self):
        processes = [
            [import_data, True, {'def_dict': self.def_dict, 'df': self.df, 'source_dir': self.dir}],
            [self.update_hierarchy, True, {'df': self.df}]
        ]
        self.wraper(self._run_processes)(processes, 'import entity_mapping')
    
    def get_entites_by_group(self, level, mask):
        entites = self.df[(self.df['L{}'.format(level)].str.startswith(mask)) & 
                        (self.df['parent / base']=='base')]['ID'].values.tolist()
        group_name = self.df.loc[self.df['L{}'.format(level)].str.startswith(mask),'L{}'.format(level)].values.tolist()[0]
        return True, 'entitiye group level: {}, group_name: {} created'.format(level, group_name), group_name, entites

    # import Entity mapping
    def update_hierarchy(self, df):
        L_list = ['','','','','','','','','','','']
        self.status, comment = True, 'update hierarchy completed'
        for idx, row in df.iterrows():
            for id in range(8,0, -1):
                if row[f'L{id}']=='nan':
                    L_list[id] == ''
                else:
                    L_list[id] = row[f'L{id}']
                    for id2 in range(1, id):
                        try:
                            df.loc[df.index==idx, f'L{id2}'] = L_list[id2]
                        except Exception as E:
                            self.status, comment = False, 'update hierarchy error. Row: {}, L{} : {}'.format(idx, id2, E)
        return self.status, comment, df
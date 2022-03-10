import pandas as pd

from ._common import ProcessingMixIn, CommonData, ComparedData
from .df_manipulation import import_data_csv, del_zero_value_rows
from .tools import file_list, status_dict


class DataBPC(CommonData, ProcessingMixIn):
    def __init__(self, def_dict, mapping_df, dir, year, period, mask='CC', ext='txt') -> None:
        super().__init__(dir, def_dict, mapping_df, year, period, mask, ext, name="BPC_data", msg=['BPC Entity data failure: ', 'BPC Entity data completed'])
        self.df = pd.DataFrame(columns=self.def_dict['columns_name'])
        self.YTD = True

    def initial_process(self):
        processes = [
            [self.load_entities_file_list, True, {}],
            [self.load_data, True, {}]
        ]
        self._run_processes(processes, 'BPC preprocessing and processing ')

    def load_entities_file_list(self):
        self.files_list = file_list(self.dir, ext=self.ext, startswith=self.mask)
        self.status_df(status_dict(True, 'imported "{}" BPC entities data files'.format(len(self.files_list)), self.files_list, level=self.wraper.debug_level))
        self._get_period()
        self.files_list.sort()
        self.files_list = [item for item in self.files_list if item[0] == self.year and item[1]==self.period]
        if self.files_list:
            self.status, comment = True, 'imported, splited and sorted {} BPC files completed'.format(len(self.files_list))
        else:
            self.status, comment = False, 'no files matched with mask "{}*.{}"'.format(self.mask, self.ext)
        return self.status, comment, self.df
    
    def _get_period(self):
        def split_BPC_period(s):
            year, period = s.split('.')
            year = int(year)
            period = int(period)
            return year, period
        
        for idx, file_name in enumerate(self.files_list):
            with open(file_name,'rt') as file:
                period_pos = file.readline().split(',').index('TIME')
                BPC_period = file.readline().split(',')[period_pos]
                self.files_list[idx] = list(split_BPC_period(BPC_period)) + [file_name]

    def load_data(self):
        processes = [
            [import_data_csv, True, {'def_dict': self.def_dict, 'df': self.df, 'file':self.files_list[0][-1]}],
            [self.data_summary, True, {'cols': ['BPC_VALUE']}],
            [self.mapping_df[0].mapping, True, {'df': self.df, 'left_on': 'ENTITY', 'right_on': 'ID'}],
            [self.mapping_df[0].check_mapping, True, {'df': self.df, 'col': 'EPM_ENTITY', 
                                                      'no_mapping_action': self.no_entity_mapping_action}],
            [self.mapping_df[1].mapping, True, {'df': self.df, 'on': 'ACC_ID'}],
            [self.mapping_df[1].check_mapping, True, {'df': self.df, 'col': 'GCAD_ID'}],
            [del_zero_value_rows, True, {'df': self.df, 'column': 'BPC_VALUE', 'limit': self.limit}],
            [self.movement_mapping_processing, True, {}],
        ]

        self._run_processes(processes, "BPC load_YTD {}/{} data ".format(self.year, self.period))
        return self.status, 'BPC loading YTD {}/{} data '.format(self.year, self.period) + 'completed' if self.status else 'failure', self.df

    def no_entity_mapping_action(self, col):
        output_file = self.OUTPUT_DIR + 'BPC_not_mapped_Entities.xlsx'
        self.df[
            self.df[col].isnull()][
                ['ENTITY','BPC_VALUE' ]
            ].groupby('ENTITY').agg(
                ['sum', 'min', 'max', 'count']
            ).to_excel(output_file)
        return True, "not mapped Entities listed in file: {}".format(output_file), self.df
        
    def compare_with_entity(self, entity_obj, output_dir='', ext='csv'):
        if not output_dir:
            output_dir = self.OUTPUT_DIR
        prefix = 'Entity_{}'.format(entity_obj.entity_id)
        self.copy_df = self.df
        processes = [
            [self.check_consistency, True, {'entity_obj': entity_obj}],
            [self.create_selected_df, True, {'entity_obj': entity_obj}],
            [self.check_values, True, {'entity_obj': entity_obj}],
            [self.create_compare_df, True, {'entity_obj': entity_obj}],
            [self.create_output_file, True, {'prefix': prefix, 'ext': ext, 'output_dir': output_dir}],
        ]

        self._run_processes(processes, 'Compare BPC with S4 ')
        if self.status:
            status, comment, compare_obj = self.wraper(ComparedData.create_compared)(self, self.name + ' with ' + entity_obj.name)
            status, comment, _ = self.wraper(compare_obj.export_to_file)(output_dir=output_dir, ext=ext, export_df=self.compare_df)
            comment = "Period {}/{}, entity {} comparison between S4 and BPC completed".format(self.year, self.period, entity_obj.entity_id)
            return self.status, comment, self.compare_df
        else:
            comment = "Period {}/{}, entity {} comparison between S4 and BPC failed".format(self.year, self.period, entity_obj.entity_id)
        return self.status, comment, self.df
    
    def create_selected_df(self, entity_obj):
        entity_id = entity_obj.entity_id
        entity_columns = entity_obj.df.columns.tolist()
        entity_columns = entity_columns[:entity_columns.index('TIME') + 1]
        my_columns = self.df.columns.tolist()
        self.BPC_S4_compare_fields = [col for col in my_columns if col in entity_columns and col !='file_sheet']
        self.selected_df = self.df[self.df['ENTITY']==entity_id][self.BPC_S4_compare_fields + ['BPC_VALUE']]
        if len(self.selected_df):
            self.status = True
            comment = "selected BPC rows for entity {}".format(entity_id)
        else:
            self.status = False
            comment = "no BPC rows for entity {}".format(entity_id)
        return self.status, comment, self.df

    def check_values(self, entity_obj):
        BPC_sum = self.selected_df['BPC_VALUE'].sum()
        S4_sum = entity_obj['S4_VALUE'].sum()
        return True, 'entity {}. BPC value sum: $ {:,.2f}, S4 value sum: $ {:,.2f}'.format(entity_obj.entity_id, BPC_sum, S4_sum)
    
    def create_compare_df(self, comp_obj, output_dir='', ext='csv'):
        report_df = pd.DataFrame({'Source': self.name} | {item: self.df[item].sum() for item in self.float_cols})
        report_df.append(pd.DataFrame({'Source': comp_obj.name} | {item: comp_obj.df[item].sum() for item in comp_obj.float_cols}))
        self.compare_df = pd.concat([
            self.selected_df.groupby(
                self.BPC_S4_compare_fields).sum(), 
            comp_obj[self.BPC_S4_compare_fields + ['S4_VALUE']].groupby(
                self.BPC_S4_compare_fields).sum()
            ], join='outer', axis=1, 
        ).reset_index()
        self.compare_df['empty'] = (self.compare_df['BPC_VALUE'].isnull() | self.compare_df['S4_VALUE'].isnull())
        self.compare_df['BPC_VALUE'] = self.compare_df['BPC_VALUE'].fillna(0)
        self.compare_df['S4_VALUE'] = self.compare_df['S4_VALUE'].fillna(0)
        self.compare_df['Diff_VALUE'] = self.compare_df['BPC_VALUE'].round(2) - self.compare_df['S4_VALUE'].round(2)
        # compare_df['empty'] = (compare_df['BPC_VALUE'].isnull() | compare_df['S4_VALUE'].isnull())
        # del(self.selected_df)
        # return True, 'merge file values: BPC: $ {:,.2f}, S4: $ {:,.2f}'.format(self.compare_df['BPC_VALUE'].sum(), self.compare_df['S4_VALUE'].sum()), self.df
        _, _, compare_obj = ComparedData.create_compared(self, self.name + ' with S4')
        report_df.append(pd.DataFrame({'Source': compare_obj.name} | {item: compare_obj.df[item].sum() for item in compare_obj.float_cols}))
        
        return compare_obj.export_to_file(output_dir=output_dir, prefix=comp_obj.name, ext=ext, df=self.compare_df)
    
    def create_output_file(self, prefix, ext, output_dir):
        return self.export_to_file(output_dir=output_dir, prefix=prefix, ext=ext, df=self.compare_df )
    
    def check_consistency(self, entity_obj):
        self.status = (entity_obj.period == self.period) and (entity_obj.year == self.year)
        if not self.status:
            comment = "S4(year={}, period={}) and BPC(year={}, period={}) not consistent. Process terminated".format(
                entity_obj.year, entity_obj.period, self.year, self.period
            )
            return self.status, comment, self.df
        return self.status, "S4 and BPC (year={}, period={}) consistency checked".format(self.year, self.period), self.df
    
    @staticmethod
    def movement_mapping(item):
        FUNC_AREA_MAPPING = {
            'AD01': 'O_OWNST',
            'AD02': 'L_OWNST',
            'AD03': 'MANUFA',
            'AD04': 'GENADM',
            'AD05': 'RESTRUCT',
            'AD06': 'NONREC',
        }
        return FUNC_AREA_MAPPING.get(item, '[None]')

    def movement_mapping_processing(self):
        # self.df.loc[:,'MOVEMENT_TYPE'] = self.df.loc[:,'FUNC_AREA'].apply(self.movement_mapping)
        # self.df.loc[:, 'MOVEMENT_TYPE'] = self.df['FUNC_AREA'].apply(self.movement_mapping)
        self.df['MOVEMENT_TYPE'] = self.df['FUNC_AREA'].apply(self.movement_mapping)
        return self.status, "Added MOVEMENT_TYPE column. Records with no numbers: {:,}.".format((self.df['MOVEMENT_TYPE']=='[None]').sum()), self.df
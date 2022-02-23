import pandas as pd

from ._common import ProcessingMixIn, CommonData
from .df_manipulation import import_data, change_columns_type, clean_empty_rows, change_nan_in_columns, del_zero_value_rows
from .tools import file_list, status_dict

class DataS4(CommonData, ProcessingMixIn):
    def __init__(self, def_dict, mapping_df, dir, entity_id, year, period, YTD=True, mask='CC', ext='txt') -> None:
        super().__init__(dir, def_dict, mapping_df, year, period, mask, ext, name="S4_data", msg=['S4 Entity data failure: ', 'S4 Entity data completed'])
        self.entity_id = entity_id
        self.df = pd.DataFrame(columns=self.def_dict['columns_name'])
        self.YTD = YTD
        processes = [
            [self.load_entities_file_list, True, {}],
            [self.load_data, True, {}]
        ]
        self._run_processes(processes, 'S4 preprocessing and processing ')
        
    def create_concat_periods(self):
        columns = self.df.columns.tolist()
        columns.remove('Period')
        columns.remove('S4_VALUE')
        self.concat_columns = columns
        return self.status, 'preparation columns for grouping periods completed', self.df
        
    def load_entities_file_list(self):
        self.status = True
        self.files_list = file_list(self.dir, ext=self.ext, startswith=self.mask)
        self.status_df(status_dict(True, 'imported "{}" S4 entities data files'.format(len(self.files_list)), self.files_list, level=self.wraper.debug_level))
        self._get_period()
        self.files_list.sort()
        if self.YTD:
            self.files_list = [item for item in self.files_list if item[0] == int(self.entity_id) and item[1]==self.year and item[2]<=self.period]
        else:
            self.files_list = [item for item in self.files_list if item[0] == int(self.entity_id) and item[1]==self.year and item[2]==self.period]
        if self.files_list:
            self.status, comment = True, 'imported, splited and sorted {} S4 entities files completed'.format(len(self.files_list))
        else:
            self.status, comment = False, 'no files matched with mask "{}*.{}"'.format(self.mask, self.ext)
        return self.status, comment, self.df
    
    def _get_period(self):
        for idx, file_name in enumerate(self.files_list):
            try:
                split_folders = file_name.split('/')
                entity = int(split_folders[-2])
                year = int(split_folders[-3])
                split_name = split_folders[-1].split('.')[0]
                period = int([item for item in split_name.split(" ") if item.startswith('P')][-1][1:])
                self.files_list[idx] = [entity, year, period] + [file_name]
            except(IndexError, ValueError) as e:
                self.status = False
                self.status_df(status_dict(False, 'wrong filie/folder structure. File: {}, error: {}'.format(file_name, e)), level=self.wraper.debug_level)

    def add_BPC_period(self, df):
        self.status, comment = True, 'BPC period added succesfully'
        try:
            df['TIME'] = df['Fiscal_Yr'].astype(str).str.strip() + '.' + df['Period'].apply(lambda x: '{:02d}'.format(x))
        except (ValueError, TypeError)  as E:
            self.status, comment = False, 'add_BPC_period failure:' + E
        return self.status, comment, df

    def load_YTD_data(self):
        columns_name = self.def_dict['columns_name']
        self.def_dict['columns_name'] = self.def_dict['YTDcolumns_name']
        columns_to_import = self.def_dict['columns_to_import']
        self.def_dict['columns_to_import'] = self.def_dict['YTDcolumns_to_import']
        columns_types = self.def_dict['columns_types']
        self.def_dict['columns_types'] = self.def_dict['YTDcolumns_types']
        period_loc = self.def_dict['columns_to_import'][-1] + 1
        for i in range(self.period):
            self.def_dict['columns_name'].append('P{}'.format(i + 1))
            self.def_dict['columns_to_import'].append(period_loc + i)
            self.def_dict['columns_types'].append(float)
        self.status, comment, self.df = self.wraper(self.load_period)(self.period, False)
        self.df['Period'] = self.period
        self.df['S4_VALUE'] = self.df[['P{}'.format(item) for item in range(self.period + 1)]].sum(axis=1)
        self.status, comment, self.df = self.wraper(self.mapping_df.mapping)(self.df, on='ACC_ID')
        self.status, comment, self.df = self.wraper(self.mapping_df.check_mapping)(self.df, 'GCAD_ID')
        output_file = self.files_list[0][3].replace(
            'YTD P{} CC{}summary'.format(self.period, self.entity_id),
            'YTD{} CC {} P{}'.format(self.year, self.entity_id, self.period)).replace('.xlsx', '.txt')
        month_col = [item for item in self.df.columns if item.startswith('P') and item!='Period']
        self.df['TIME'] = '{}.{}'.format(self.year, self.period)
        for column in columns_name:
            if not column in self.def_dict['columns_name']:
                self.df[column] = 'none'
        self.def_dict['columns_name'] = columns_name
        self.def_dict['columns_to_import'] = columns_to_import
        self.def_dict['columns_types'] = columns_types
        self.df[self.def_dict['columns_name'] + month_col + ['TIME']].to_csv(output_file, index=False)
        if self.status:
            comment = 'imported and converted YTD data from file {}. Total sum: ${:,.2}'.format(self.files_list[0][3], self.df['S4_VALUE'].sum())
        return self.status, comment, self.df
        
    def load_data(self):
        if self.mask.startswith('YTD'):
            return self.load_YTD_data()
        if self.YTD:
            output_df = pd.DataFrame(columns = self.concat_columns)
            periods_list = []
            full_scope = False
            for curr_period in range(self.period + 1):
                periods_list.append([self.entity_id, self.year, curr_period, '{}_{}'.format(self.year, curr_period), 0])
                if self.status:
                    self.status, comment, self.df = self.wraper(self.load_period)(curr_period, full_scope)
                else:
                    break
                periods_list[-1][4] = round(self.df['S4_VALUE'].sum(), 2)
                self.df.drop('Period', axis='columns', inplace=True)
                self.df.rename(columns={'S4_VALUE': '{}_{}'.format(self.year, curr_period)}, inplace=True)
                output_df = pd.concat(
                    [output_df.groupby(self.concat_columns).sum(),
                    self.df.groupby(self.concat_columns).sum()],
                    join='outer', axis=1
                )
                self.df = None
            output_df = output_df.reset_index()
            for item in periods_list:
                output_df[item[3]] = output_df[item[3]].fillna(0) 
            output_df['S4_VALUE'] = output_df[['{}_{}'.format(period[1], period[2]) for period in periods_list]].sum(axis=1)
            output_df['Period'] = self.period
            import_file_sum = round(sum([item[4] for item in periods_list]), 2)
            combained_sum = round(output_df['S4_VALUE'].sum(), 2)
            self.status_df(
                status_dict(
                    import_file_sum == combained_sum, 'combained {} entity {} files. sum per files: ${:,.2f}, sum combined file: ${:,.2f}'.format(
                    len(periods_list), self.entity_id, import_file_sum, combained_sum), level=self.wraper.debug_level
                )
            )
            self.status, comment, output_df = self.wraper(self.mapping_df.mapping)(output_df, on='ACC_ID')
            self.status, comment, output_df = self.wraper(self.mapping_df.check_mapping)(output_df, 'GCAD_ID')
            if self.status:
                comment = 'merge {} entity {}files completed'.format(len(self.files_list), self.entity_id)
                output_file = self.files_list[-1][-1].replace('CC', 'YTD_CC').replace('.xlsx', '.txt')
                month_col = [item for item in output_df.columns if item.startswith('{}_'.format(self.year))]
                output_df['TIME'] = '{}.{}'.format(self.year, self.period)
                output_df[self.def_dict['columns_name'] + month_col + ['TIME']].to_csv(output_file, index=False)
                self.df = output_df
        else:
            self.status, comment, self.df = self.wraper(self.load_period)(self.period, True)
        comment = "{} - loaded data completed".format(self.name) if self.status else "{} - laded data falied".format(self.name)
        return self.status, comment, self.df

    def load_period(self, curr_period, full_scope=True):
        self.def_dict['file'] = [item[3] for item in self.files_list if item[2]==curr_period][0]
        processes = [
            [import_data, True, {'def_dict': self.def_dict, 'df': self.df, 'na_filter':True}],
            [clean_empty_rows, True, {'df': self.df, 'col': 1}], 
            [change_columns_type, True, {'df': self.df, 'types': self.def_dict.get('columns_types',None)}],
            [self.add_BPC_period, full_scope, {'df': self.df}],
            [self.mapping_df.mapping, full_scope, {'df': self.df, 'on': 'ACC_ID'}],
            [self.mapping_df.check_mapping, full_scope, {'df': self.df, 'col': 'GCAD_ID'}],
            [change_nan_in_columns, True, {'df': self.df, 'columns': ['FUNC_AREA', 'PROFIT_CTR', 'STORE_DEPT', 'INTERCO']}], 
            [del_zero_value_rows, True, {'df': self.df, 'column': 'S4_VALUE', 'limit': self.limit}],
        ]
        self._run_processes(processes, "load_period {}/{} data for entity {} ".format(self.year, curr_period, self.entity_id))
        return self.status, 'loading period {}/{} data for entity {} '.format(self.year, curr_period, self.entity_id) + 'completed' if self.status else 'failure', self.df
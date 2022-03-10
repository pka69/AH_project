import pandas as pd

from ._common import ProcessingMixIn, CommonData, ComparedData
from .df_manipulation import import_data, compare_values
from .tools import file_list, status_dict



class DataEPM(CommonData, ProcessingMixIn):
    def __init__(self, def_dict, mapping_df, dir, year, period, mask='CC', ext='txt') -> None:
        super().__init__(dir, def_dict, mapping_df, year, period, mask, ext, name="EPM_data", msg=['EPM Entity data failure: ', 'EPM Entity data completed'])
        self.df = pd.DataFrame(columns=self.def_dict['columns_name'])
        self.YTD = True

    def initial_process(self):
        processes = [
            [self.load_EPM_file_list, True, {}],
             [self.load_data, True, {}],
             [self.change_value, True, {}],
             [self.del_I_on_the_end_of_GCAD_ID, True, {}],
             [self.add_mapping_columns, True, {}],
            #  [self.load_data, True, {}],
        ]
        self._run_processes(processes, 'BPC preprocessing and processing ')

    def load_EPM_file_list(self):
        self.status = True
        self.files_list = file_list(self.dir, ext=self.ext, startswith=self.mask)
        self.status_df(status_dict(True, 'imported "{}" EPM entities data files'.format(len(self.files_list)), self.files_list))
        self._get_period()
        self.files_list.sort()
        if self.files_list:
            self.status, comment = True, 'imported, splited and sorted {} EPM files completed'.format(len(self.files_list))
        else:
            self.status, comment = False, 'no files matched with mask "{}"'.format(self.mask)
        return self.status, comment, self.files_list

    def _get_period(self):
        for idx, file_name in enumerate(self.files_list):
            try:
                year = int(file_name.split('/')[-1].split('.')[0].split('_')[0])
                period = int(file_name.split('/')[-1].split('.')[0].split('_')[1][1:])
                self.files_list[idx] = [year, period, file_name]
            except Exception:
                self.status = False

    def load_data(self):
        self.def_dict['file'] = [item[-1] for item in self.files_list if item[0]==self.year and item[1]==self.period][0]
        return import_data(self.def_dict, '')

    def change_value(self):
        self.df['EPM_VALUE'] = - self.df['EPM_VALUE']
        return self.status, 'making negatve values completed. Total EPM_VALUE={:,.2f}. No records: {:7,}'.format(self.df['EPM_VALUE'].sum(), self.df['EPM_VALUE'].count()), self.df

    def del_I_on_the_end_of_GCAD_ID(self):
        # filter accountas with I on the end
        EPM_filter = self.df['GCAD_ID'].str.endswith('I')
        #EPM usuwanie I na końcu konta
        self.df.loc[EPM_filter, 'GCAD_ID'] = self.df.loc[EPM_filter, 'GCAD_ID'].str[:-1]
        return self.status, 'changed GCAD_ID with I on the end. number of records: {:5,}'.format(EPM_filter.sum()), self.df

    def add_mapping_columns(self):
        try:
            self.df = self.df.merge(
                self.mapping_df.df[self.mapping_df.df['ACC_GROUP']!='ALLC'][['GCAD_ID', 'ACC_GROUP', 'ACC_ID']].groupby(
                    ['GCAD_ID', 'ACC_GROUP']).count().reset_index(), 
                    how = 'left', on='GCAD_ID' )
            _, _, self.df = self.mapping_df.fill_no_match(self.df)
            self.df.drop('ACC_ID', axis='columns', inplace=True)
        except Exception as e:
            self.status = False
            return self.status, "EPM GCAD mapping failure: {}".format(e), self.df
        return self.status, 'GCAD mapping added. Total EPM_VALUE={:,.2f}. No of records: {:5,}'.format(self.df['EPM_VALUE'].sum(), self.df['EPM_VALUE'].count()), self.df
    
    def compare_with_BPC(self, comp_obj, output_dir='', ext='xlsx', 
                        left_group_by = ['EPM_ENTITY', 'GCAD_ID', 'ACC_GROUP','MOVEMENT_TYPE', 'EPM_VALUE'],
                        right_group_by = ['EPM_ENTITY', 'GCAD_ID', 'ACC_GROUP','MOVEMENT_TYPE', 'BPC_VALUE']
    ):
        report_df = pd.DataFrame({'Source': [self.name]} | {item: [self.df[item].sum()] for item in self.float_cols})
        report_df =report_df.append(pd.DataFrame({'Source': [comp_obj.name]} | {item: [comp_obj.df[item].sum()] for item in comp_obj.float_cols}), ignore_index=True)
        self.compare_df = pd.concat([
            self.df[left_group_by].groupby(left_group_by[:-1]).sum(), 
            comp_obj.df[right_group_by].groupby(right_group_by[:-1]).sum()]
            , join='outer', axis=1, 
        ).reset_index()
        test = pd.merge(
            self.df[left_group_by].groupby(left_group_by[:-1]).sum().reset_index(), 
            comp_obj.df[right_group_by].groupby(right_group_by[:-1]).sum().reset_index(),
            how='outer', left_on=left_group_by[:-1], right_on=right_group_by[:-1]
        )
        # compare_values(self.df, self.compare_df, left_group_by[:-1], left_group_by[-1])
        self.compare_df['empty'] = (self.compare_df['BPC_VALUE'].isnull() | self.compare_df['EPM_VALUE'].isnull())
        self.compare_df['BPC_VALUE'] = self.compare_df['BPC_VALUE'].fillna(0)
        self.compare_df['EPM_VALUE'] = self.compare_df['EPM_VALUE'].fillna(0)
        self.compare_df['Diff_VALUE'] = self.compare_df['EPM_VALUE'].round(2) - self.compare_df['BPC_VALUE'].round(2)
        _, _, compare_obj = self.wraper(ComparedData.create_compared)(self, self.name + ' with ' + comp_obj.name)
        report_df =report_df.append(pd.DataFrame({'Source': [compare_obj.name]} | {item: [compare_obj.df[item].sum()] for item in compare_obj.float_cols}))
        status, comment, _ = self.wraper(compare_obj.export_to_file)(output_dir=output_dir, ext=ext, export_df=self.compare_df)
        
        return status, comment, compare_obj
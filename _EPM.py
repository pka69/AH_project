import pandas as pd

from ._common import ProcessingMixin, CommonData, ComparedData
from .df_manipulation import import_data
from .tools import file_list, status_dict



class DataEPM(CommonData, ProcessingMixin):
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
            self.status, comment = False, 'no files matched with mask "{}"'.format(mask)
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
            self.df.drop('ACC_ID', axis='columns', inplace=True)
        except Exception as e:
            self.status = False
            return self.status, "EPM GCAD mapping failure: {}".format(e), self.df
        return self.status, 'GCAD mapping added. Total EPM_VALUE={:,.2f}. No of records: {:5,}'.format(self.df['EPM_VALUE'].sum(), self.df['EPM_VALUE'].count()), self.df
    
    def compare_with_BPC(self, bpc_obj, output_dir='', ext='xlsx', 
                        EPM_group_by = ['EPM_ENTITY', 'GCAD_ID', 'ACC_GROUP','MOVEMENT_TYPE', 'EPM_VALUE'],
                        BPC_group_by = ['EPM_ENTITY', 'GCAD_ID', 'ACC_GROUP','MOVEMENT_TYPE', 'BPC_VALUE']
    ):
        self.compare_df = pd.concat([
            self.df[EPM_group_by].groupby(EPM_group_by[:-1]).sum(), 
            bpc_obj.df[BPC_group_by].groupby(BPC_group_by[:-1]).sum()]
            , join='outer', axis=1, 
        ).reset_index()
        _, _, compare_obj = ComparedData.create_compared(self, self.name + ' with BPC')
        return compare_obj.export_to_file(output_dir=output_dir, prefix=bpc_obj.name, ext=ext, df=self.compare_df)
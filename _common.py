from abc import ABC, abstractmethod
import pandas as pd


def dummy(func, *args, **kwargs):
    return func(args, kwargs)

class ProcessingMixIn:
    def _processing(self, processes):
        self.status, comment = True, 'initialize prosessing...'
        for action in processes:
            if self.status and action[1]:
                if 'df' in action[2].keys(): action[2]['df'] = self.df
                self.status, comment, self.df = self.wraper(action[0])(**action[2])
        if self.status:
            comment="processing ({}) completed".format(len(processes))
        else:
            # self.msg[0] = self.msg[0] + comment
            comment="processing failure"
        return self.status, comment
    
    def _run_processes(self, processes, std_comment):
        self.status, comment = self.wraper(self._processing)(processes)
        if self.status:
            comment = std_comment + " completed"
        else:
            comment = std_comment + " error"
        return self.status, comment
    
class Common_df(ABC):
    wraper = dummy
    status_df = lambda x: x
    limit = 0.0001
    OUTPUT_DIR = 'output/'
    # not_wrap = ['status_df', 'wraper', 'create']
    def __init__(self, name='dummy _abstract', msg=['', '']) -> None:
        super().__init__()
        self.df = pd.DataFrame()
        self.filter_df = None
        self.name = name
        self.msg = msg
        self.status = True
        self.float_cols = []

    @abstractmethod
    def initial_process(self):
        pass
    
    def report_success(self):
        if self.status:
            comment = f'{self.name}: {self.msg[self.status]}, ' + ','.join([f'{item} = {self.df[item].sum():,.2f}' for item in self.float_cols]) if self.float_cols else ''
        else:
            comment = f'{self.name}: {self.msg[self.status]}. '
        return self.status, comment, self
    
    def __call__(self):
        return self.df
    
    def __getitem__(self, key):
        return self.df[key]
    
    def create_float_values_attr(self):
        columns = [item for item in self.df.columns if item.endswith('_VALUE')]
        dataTypeSeries = self.df.dtypes
        
        for idx, item in enumerate(columns):
            if dataTypeSeries[item] == float:
                self.float_cols.append(item)
    
    @classmethod
    def create(cls, *args, **kwargs):
        obj = cls(*args, **kwargs)
        obj.initial_process()
        obj.create_float_values_attr()
        return obj.report_success()
    
class CommonData(Common_df):
    def __init__(self,  dir, def_dict, mapping_df,year, period, mask, ext, name='CommonData_abstract', msg=['', ''], **kwargs) -> None:
        super().__init__(name, msg)
        self.mapping_df = mapping_df
        self.def_dict = def_dict
        self.dir = dir
        self.period = period
        self.year = year
        self.mask = mask
        self.ext = ext
        self.YTD = False
        self.compare_df = None
        self.df = pd.DataFrame(columns=def_dict['columns_name'])
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def filter(cls, origin, column, value):
        new_obj = cls(
            dir=dir,
            def_dict=origin.def_dict,
            mapping_df=origin.mapping_df,
            year=origin.year,
            period=origin.period,
            mask=origin.mask,
            ext=origin.ext
        )
        members = [attr for attr in dir(origin) if not callable(getattr(origin, attr)) and not attr.startswith("_")]
        for attr in members:
            if attr == 'df':
                continue
            setattr(new_obj, attr, getattr(origin, attr))
        new_obj.df = origin.df[origin.df[column]==value]
        new_obj.msg = ['filtered ' + item for item in new_obj.msg]
        new_obj.name = new_obj.name + '[{} = {}]'.format(column, value)
        return new_obj.report_success()

    def export_to_file(self, output_dir='', prefix='', ext='csv', export_df = pd.DataFrame()):
        xls_ext = ['xls', 'xlm', 'xlsx', 'xlsm']
        if not output_dir:
            output_dir = self.OUTPUT_DIR
        export_file_name = '{}{}{}{} P{} - compare {}.{}'.format(output_dir, prefix, self.year, 'YTD' if self.YTD else '', self.period, self.name, ext)
        if export_df.empty:
            export_df = self.df
        comment = 'file {} succesfully exported.'.format(export_file_name)
        success = True
        try:
            if ext in xls_ext:
                export_df.to_excel(export_file_name, index=False)
            else:
                export_df.to_csv(export_file_name, index=False)
        except Exception as e:
            success = False
            comment = 'file {} export failed: {}.'.format(export_file_name, str(e))
        return success, comment, self.df
    
    def sum_values(self):
        values = self.df[self.float_cols].sum().tolist()
        return ', '.join(['{}: {:,.2f}'.format(col, value) for col, value in zip(self.float_cols, values)])
class ComparedData(CommonData):
    def initial_process(self):
        pass

    @classmethod
    def create_compared(cls, source_obj, name):
        new_obj = cls(
            dir='',
            def_dict=source_obj.def_dict,
            mapping_df=None,
            year=source_obj.year,
            period=source_obj.period,
            mask='',
            ext='',
            name=name
        )
        new_obj.def_dict=[]
        new_obj.YTD = source_obj.YTD
        new_obj.df = source_obj.compare_df
        new_obj.msg = ['compared {} failed,'.format(new_obj.name), 'compared {} created succesfully.'.format(new_obj.name)]
        new_obj.create_float_values_attr()
        return new_obj.report_success()
    
    def summary(self, columns=[]):
        if isinstance(columns, str):
            columns = [columns]
        return str(self.df[columns + self.float_cols].groupby(columns).sum())

class MappingData(Common_df):
    def __init__(self, name='mapping_abstract', msg=['','']) -> None:
        super().__init__(name, msg)
        self.mapping_columns = []

    def mapping(self, df, on=None, left_on=None, right_on=None):
        try:
            df = df.merge(self.df[self.mapping_columns], # , 'REPORT_TREE'
                        how = 'left', on=on, left_on=left_on, right_on=right_on )
            for item in self.mapping_columns:
                if self.df[item].dtypes!=df[item].dtypes:
                    try:
                        df[item].astype(self.df[item].dtypes)
                    except:
                        int_types_conv = {
                            'int8': 'Int8',
                            'int16': 'Int16',
                            'int32': 'Int32',
                        }
                        df[item].astype(int_types_conv[str(self.df[item].dtypes)])
            if self.float_cols:
                self.status, comment = True, '{} mapping added. Total {}={:,.2f}. No of records: {:,5}'.format(
                    on if on else left_on, self.float_cols[-1], self.df[self.float_cols[-1]].sum(), self.df[self.float_cols[-1]].count())
            else:
                self.status, comment = True, self.msg[1]
        except Exception as e:
            self.status, comment = False, self.msg[0] + '{}'.format(e)
        return self.status, comment, df
    
    def check_mapping(self, df, col):
        self.status, comment = True, 'GCAD mapping checked completness'
        no_mapping = df[col].isnull().sum()
        if no_mapping:
            # self.status = False
            comment = '{} accounts are out of mapping in entity file "{}". Check it!'.format(no_mapping, df['file_sheet'][0])
        return self.status, comment, df
    

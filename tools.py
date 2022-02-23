import os
from datetime import datetime, timedelta
import psutil
import pandas as pd

def file_list(directory, ext = ".csv", startswith = ""):
    '''
    return file list with defined extension and startswith
    including subfloders files
    '''
    return [
            directory + f for f in os.listdir(directory) 
            if os.path.isfile(os.path.join(directory, f)) 
            and f.startswith(startswith)
            and f.endswith(ext)
            ] + [file for sublist in [
                file_list(directory + subdir + '/', ext, startswith) for subdir in os.listdir(directory) if 
                 not os.path.isfile(os.path.join(directory, subdir))] for file in sublist]

def check_folder_structure(folder_list):
    my_folder_list = [f for f in os.listdir()
            if not os.path.isfile(os.path.join(f))]
    for directory in folder_list:
        if (directory[:-1] if directory[-1] in ['\\', '/'] else directory) not in my_folder_list:
            os.mkdir(directory[:-1] if directory[-1] in ['\\', '/'] else directory)

last_process='main'

def status_dict(status, comment, additional_str='', additional_value = None, level = -1, process=''):
    global last_process
    if process:
        last_process = process
    else:
        process = last_process
    return {
        'date_time': [datetime.now()],
        'status': [status], 
        'comment': [comment], 
        'additional_info': [additional_str],
        'additional_value': [additional_value],
        'level': [level],
        'process': process
    }

class StatusWraper:
    wraper = None
    level = 0
    def __init__(self, status_df, debug_level=0) -> None:
        self.status_df = status_df
        self.debug_level = debug_level

    def __call__(self, func):
        if not self.debug_level:
            return func
        def wraper( *args, **kwargs):
            StatusWraper.level += 1
            result = func(*args, **kwargs)
            self.status_df.add_status(
                status_dict(status=result[0], comment=result[1], process= func.__name__, level=StatusWraper.level)
            )
            StatusWraper.level -= 1
            return result
        return wraper

    def change_debug_level(self, debug_level):
        self.debug_level = debug_level
        self.status_df.change_debug_level(debug_level)

    @classmethod
    def wraper_get_or_create(cls, status_df, debug_level=0):
        if not cls.wraper:
            cls.wraper = cls(status_df, debug_level)
        return cls.wraper
    
    def __str__(self) -> str:
        return 'wraper defined.\ndebug level {}.\nstatus {} '.format(self.debug_level, 'defined' if self.status_df else 'not defined')
    
class StatusDF():
    def __init__(self, output_file, debug_level=0):
        self.debug_level = debug_level
        self.last_time = datetime.now()
        self.last_level = 0
        self.status_df = pd.DataFrame(
            data={
                'date_time': [self.last_time],
                'time_delta': [timedelta(seconds=0)],
                'status': [True], 
                'comment': ['status file initialization'], 
                'additional_info': [''],
                'additional_value':[ 0.00],
                'level': 0,
                'process': ''
            },
            columns=['date_time', 'process', 'level', 'status', 'comment', 'additional_info', 'additional_value', 'time_delta'],
        )
        # pd.options.display.float_format = "{:,.2f}".format
        self.status_df.style.format({
            'additional_value': lambda x: '{:,.2f}'.format(x),
            'time_delta': lambda t: t.strftime("%M:%S.%SS")
        })
        self.output_file = output_file

    def __call__(self, status_dict):
        self.add_status(status_dict)

    def change_debug_level(self, debug_level):
        self.debug_level = debug_level

    def add_status(self, status_dict):
        time_delta = status_dict['date_time'][0] - self.last_time
        status_dict['time_delta'] = [time_delta]
        status_dict['memory usage'] = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
        status_dict['used memory percentage'] = psutil.virtual_memory()[2]
        self.status_df = pd.concat(
            [self.status_df, pd.DataFrame.from_dict(status_dict)], ignore_index=True)
        self.last_time = status_dict['date_time'][0]
        if status_dict['level'][0]==-1:
            status_dict['level'] = [self.last_level]
        else:
            self.last_level = status_dict['level'][0]
        self.save_status()
        if self.debug_level > status_dict['level'][0]:
                print('{:%H:%M:%S} ({}) status: {} - {}'.format(
                  status_dict['date_time'][0], status_dict['level'][0], status_dict['status'][0], status_dict['comment'][0],
                ))
        return self.status_df

    @property
    def get_status(self):
        return self.status_df

    def save_status(self):
        self.status_df.to_excel(self.output_file)
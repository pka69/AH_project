import pandas as pd


# update nan in PROFIT_CTR
def change_nan_in_columns(df, columns):
    if not isinstance(columns, list):
        columns = [columns]
    status, comment = True, 'update empty value in columns({}) completed'.format(', '.join(columns))
    df_columns = df.columns
    columns = [item for item in columns if item in df_columns]
    try: 
        for column in columns:
            df.loc[(df[column]=='nan') | (df[column].isna()),column] = "NO_{}".format(column)
    except (ValueError, TypeError) as e:
        status, comment = False, 'update empty value in columns failure: {}'.format(e)
    return status, comment, df

#delete rows with zero value
def del_zero_value_rows(df, column, limit):
    if not column in df.columns:
        status , comment = True, 'lack of column {} in dataset'.format(column)
        return status, comment, df
    deleted_rows = (df[column].abs()<limit).sum()
    if deleted_rows:
        df = df[df[column].abs()>=limit]
        status, comment = True, 'deleted {} rows with value <+/-{}'.format(deleted_rows, limit)
    else:
        status, comment = True, 'no zero rows in datasheet'
    return status, comment, df

def drop_duplicates(df, columns):
    status, comment = True, 'drop duplicates in dataframe column(s) {} completed'.format( columns)
    try:
        df = df.drop_duplicates(['ACC_ID', 'GCAD_ID'])
    except Exception as e:
        status, comment = False, 'drop duplicates {}[{}] error: {}'.format(df.__name__, columns, e.data)
    return status, comment, df

def change_columns_type(df, types):
    status, comment = True, 'columns types changed completed'
    if types:
        columns = df.columns[:len(types)]
        for id, col in enumerate(columns):
            try:
                if types[id] != str and type(df[col][0]) == str:
                    df[col] = df[col].str.replace(' ','')
                    df[col] = df[col].fillna(0)
                df[col] = df[col].astype(types[id])  # , errors='ignore'
            except Exception as E:
                status, comment = False, 'column {} conversion failed to {}: {}'.format(col, types[id], E)
            if types[id] == str:
                df[col] = df[col].str.strip()
    return status, comment, df

def import_data_csv(def_dict, file, df=None, source_dir =''):
    status, comment = True, 'import csv completed'
    f_name = source_dir + file
    f_columns = def_dict.get('columns_name',None)
    f_columns_types = def_dict.get('columns_types',None)
    f_skip_rows = def_dict.get('start_data_row',1) - 1
    header = None if f_columns else 0
    output = df
    output = pd.read_csv(f_name,  # usecols = f_columns_to_import,
                         names=f_columns, header = header, skiprows = f_skip_rows, dtype=str)  # 
    output['file_sheet'] = f_name
    if f_columns_types and status:
        status, comment, output = change_columns_type(output, f_columns_types)
    return status, comment, output

def clean_empty_rows(df, col):
    status, comment = True, 'Entity empty rows cleaning completed'
    try:
        df = df[df.iloc[:,1]!=""]
        df = df[~df.iloc[:,1].isnull()]
        df = df[df.iloc[:,1]!='nan']
    except Exception as E:
        status, comment = False, 'clean_empty_rows failure:' + E
    return status, comment, df

def import_data(def_dict, source_dir='', df = None, na_filter=True, clean_by_col=None):
    ext = def_dict['file'].split('.')[-1]
    status, comment = True, 'import {} completed'.format(ext)
    f_name = source_dir + def_dict['file'] if source_dir else def_dict['file']
    f_data_sheet = def_dict.get('sheet', 0)
    f_skip_rows = def_dict.get('start_data_row',1) - 1
    f_columns_to_import = def_dict.get('columns_to_import',None)
    f_columns = def_dict.get('columns_name',None)
    f_columns_types = def_dict.get('columns_types',None)
    header = None if f_columns else 0 
    output = df
    if type(f_data_sheet) in (str, int):
        if not ext.startswith('xls'):
            output = pd.read_csv(f_name, usecols = f_columns_to_import,
                                names=f_columns, skiprows = f_skip_rows, header = header, na_filter = na_filter, 
                                na_values = '', dtype=str)
        else:    
            try:
                output = pd.read_excel(f_name, sheet_name = f_data_sheet, usecols = f_columns_to_import,
                                    names=f_columns, skiprows = f_skip_rows, header = header, na_filter = na_filter, 
                                    na_values = '', dtype=str)
            except Exception as E:
                status, comment = False, 'import file {} not finished. {}'.format(f_name, E)
        output = output[~output.isnull().all(1)]
        output['file_sheet'] = f_name
    else:
        output = pd.DataFrame()
        for sheet in f_data_sheet:
            try: 
                temp_df = pd.read_excel(f_name, sheet_name = sheet, usecols = f_columns_to_import,
                                        names=f_columns, skiprows = f_skip_rows, header = header, na_filter = na_filter, na_values = '')
                temp_df = temp_df[~temp_df.isnull().all(1)]
                temp_df['file_sheet'] = f_name + ' - ' + sheet
                output = output.append(temp_df, ignore_index=True)
            except Exception as E:
                status, comment = False, 'import file {} not finished. {}'.format(f_name, E)
    if clean_by_col and status:
        status, comment, output = clean_empty_rows(output, clean_by_col)
    if f_columns_types and status:
        status, comment, output = change_columns_type(output, f_columns_types)
    if status:
        comment = 'import data from excel file {} completed'.format(f_name)
    return status, comment, output

def compare_values(df1, df2, cols, check_col):
    cols_list = set(df1[cols].groupby(cols).count().index.tolist())
    cols_list = cols_list | set(df2[cols].groupby(cols).count().index.tolist())
    cols_list = list(cols_list)
    output_df = pd.DataFrame(columns=cols + ['df1', 'df2'])
    for item in cols_list:
        s1 = df1[df1[cols] == item][cols + [check_col]].groupby(cols).sum()
        s1 = 0 if s1.empty else s1[check_col].tolist()[0]
        s2 = df2[df2[cols] == item][cols + [check_col]].groupby(cols).sum()
        s2 = 0 if s2.empty else s2[check_col].tolist()[0]
        
        if abs(s1-s2)>0.001:
            output_df.append(item + [s1, s2])
    return output_df



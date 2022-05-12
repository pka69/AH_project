import numpy as np

MAPPING_DIR = 'source_data/'
BPC_DIR = 'BPC_row_data/'
S4_DIR = 'S4_row_data/'
OUTPUT_DIR = 'output/'
EPM_DIR = "EPM_data/"

FOLDER_LIST = [MAPPING_DIR, BPC_DIR, S4_DIR, OUTPUT_DIR, EPM_DIR]

# file format Entities
ENTITIES_FORMAT = {
    'columns_name': [
            'ENTITY', 'CCodeCurr', 'Fiscal_Yr', 'Period', 'CoA', 'ACC_ID', 'PROFIT_CTR', 'Cost_Ctr',
            'STORE_DEPT', 'Tax_Jur', 'FUNC_AREA', 'INTERCO', 'S4_VALUE', 'RPTCURRENCY',	'DB_Rows'
    ],
    'columns_types':[
            str, str, np.int16, np.int8, str, np.int32, str, str, 
            str, str, str, str, float, str, np.int32
    ],
    'start_data_row': 2,
    'columns_to_import':list(range(0,15)),
    'YTDcolumns_name': [
            'Fiscal_Yr', 'RPTCURRENCY', 'ACC_ID', 'ENTITY', 'Cost_Ctr', 'PROFIT_CTR', 'FUNC_AREA', 'INTERCO' , 'P0'
            # 'ENTITY', 'CCodeCurr', 'Fiscal_Yr', 'Period', 'CoA', 'ACC_ID', 'PROFIT_CTR',	'Cost_Ctr',
            # 'STORE_DEPT', 'Tax_Jur', 'FUNC_AREA', 'INTERCO', 'S4_VALUE', 	'DB_Rows'
    ],
    'YTDcolumns_types':[
            np.int16, str, np.int32, str, str, str, str, 
            str , float
    ],
    'YTDcolumns_to_import' :[2, 7, 15, 16, 17, 18, 19, 27, 29]
}

S4_CONCAT_COLUMNS = ['ENTITY', 'CCodeCurr', 'Fiscal_Yr', 'CoA', 'ACC_ID', 'PROFIT_CTR',	'Cost_Ctr',
            'STORE_DEPT', 'Tax_Jur', 'FUNC_AREA', 'INTERCO', 'RPTCURRENCY',	'DB_Rows']

ENTITIES_YTDFORMAT = {
    'columns_name': [
            'Fiscal_Yr', 'RPTCURRENCY', 'ACC_ID', 'ENTITY', 'Cost_Ctr', 'PROFIT_CTR', 'FUNC_AREA', 'INTERCO' # , 'P0'
            # 'ENTITY', 'CCodeCurr', 'Fiscal_Yr', 'Period', 'CoA', 'ACC_ID', 'PROFIT_CTR',	'Cost_Ctr',
            # 'STORE_DEPT', 'Tax_Jur', 'FUNC_AREA', 'INTERCO', 'S4_VALUE', 	'DB_Rows'
    ],
    'columns_types':[
            np.int16, str, np.int32, str, str, str, str, 
            str  # , float
    ],
    'start_data_row': 2,
    'columns_to_import' :[2, 7, 15, 16, 17, 18, 19, 27], # , 29
}

# file format BPC
BPC_FORMAT = {
    'columns_name': ['ACC_ID', 'AUDITTRAIL', 'CATEGORY', 'CHANNEL', 'ENTITY', 'FLOW','FUNC_AREA', 'GROUPS', 
                     'INTERCO', 'PROFIT_CTR', 'LC_CURRENCY', 'STORE_DEPT', 'TIME', 'BPC_VALUE'],
    'columns_types':[np.int32, str, str, str, str, str, str, str, str, str, str, str, str, float],  # np.int16
    'start_data_row': 2,
    
}

BPC_S4_COMPARE_FIELDS = [
    'ENTITY', 'ACC_ID', 'PROFIT_CTR', 'TIME', 'INTERCO',  'FUNC_AREA', # 'STORE_DEPT', - based on YTD file without store department  
    'GCAD_ID', 'ACC_GROUP', 'TYPE', 'ACCOUNT_NAME', 'ACCOUNT_NAME_LONG']  # 'RPTCURRENCY', wyrzucone, bo w BPC jest tam LC

# BPC_S4_COMPARE_FIELDS = [
#     'ENTITY', 'ACC_ID', 'PROFIT_CTR', 'TIME', 'INTERCO',  'FUNC_AREA', 'STORE_DEPT',  
#     'GCAD_ID', 'ACC_GROUP', 'TYPE', 'ACCOUNT_NAME', 'ACCOUNT_NAME_LONG']  # 'RPTCURRENCY', wyrzucone, bo w BPC jest tam LC


# EPM file format
EPM_FORMAT = {
    'columns_name': ['Year', 'Period', 'Period_type', 'EPM_ENTITY', 'CURRENCY', 'GCAD_ID', 'IC_PARTNER', 'MOVEMENT_TYPE', 'EPM_VALUE'],
    'columns_types':[np.int16, str, str, str, str, str, str, str, float],
    'start_data_row': 1,
    'columns_to_import' :[1, 2, 3, 4, 5, 6, 7, 8, 12],
}

# file format OCAD
OCAD_FORMAT = {
    'file': 'Plan Kont.xlsx',
    'sheet': ['Operational Account T_OCAD 1', 'Opernal Bank Account T_OCAD 2'],
    'start_data_row': 9,
    'columns_name': ['ACC_ID', 'GCAD_ID', 'ACC_GROUP', 'TYPE',  'LANG', 
                     'ACCOUNT_NAME', 'ACCOUNT_NAME_LONG'],
    'columns_to_import' :[3, 4, 6, 8, 9, 10, 11],
    'columns_types': [np.int32, str, str, str, str, str, str]
}

# file format GCAD
GCAD_FORMAT = {
    'file': 'Plan Kont.xlsx',
    'sheet': 'Group Account Template_GCAD',
    'start_data_row': 9,
    'columns_name': ['GCAD_ID', 'GROUP', 'TYPE', 'LANG', 'ACCOUNT_NAME', 'ACCOUNT_NAME_LONG'],
    'columns_to_import' :[4,5,6,7,8,9], 
    'columns_types': [str, str, str, str, str, str]
}

ENTITIES_MAPPING = {
    'file': 'Entity mapping_v2.xlsx',
    'sheet': 'BPC ENTITY as of 8_10_21 -Clean',
    'start_data_row': 3,
    'columns_name': ['MATCHED ENTITY', 'ID within ARA', 'No', 'parent / base', 'ID', 'ID name', 'description',
                     'H1', 'EPM Reporting Entity', 'Levels', 'Base / parent', 'Group_Currency', 
                    'L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'L8', 'EPM_ENTITY'],
    'columns_to_import' : list(range(6)) + [7] + list(range(9,14)) + list(range(28,36)) + [43], # + list(range(9,14)) - zły mapping
    'columns_types':[str, str, np.int16, str, str, str, str, str, str, np.int8, str, str, 
                  str, str, str, str, str, str, str, str]
}

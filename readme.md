# AH project. 

## Table of contnets
* [General Info](#general-info)
* [Content](#content)
* [Technologies](#technologies)
* [Contact Info](#contact-info)
* [Project Timeline](#project-timeline)
* [Project Future](#project-future)

## General info
* this project was created to support checking consistency of data between three backoffice and reporting systems
* it is a base for Jupyter Notebook scripts used to check data consistency

## Content
* _common.py - basic objects for other dedicated to different source system. 
    * class list and function:
        * ProcessingMixIn - mixin for automate sequence of preprocessing and processing data
        main functions: _processing, _run_processes
        * Common_df(abstract class) - basic class used in two types of object: mapping oriented and data oriented
            * define class variables used in whole project:
                * wraper: if exist - adding additional pre iand post processing(in project used to register operation status in status file)
                * status_df: if exist - responsibble for register in status file process details
                * debug_level - define level of printing operations status. If debug_level =0, no reports anything, na register status file
            * initial_process - abstract function. Run in __init__
            * report_success - standarized way to report output (status, comment, and DataFrame) 
            * __call__, __getitem__ - overrided standar methods to sed back self.df data
            * create - @classmethod responsible for creation objects 
        * CommonData - basic class used in data oriented objects
            * filter - @classmethod filter data and create a new object
            * export_to_file - export dataframe in specified format and output_file_name.
            * sum_values - simply string report for all float columns in DataFrame 
        * MappingData - basic class used in mapping oriented objects
            * mapping - merge external df with mapping table based on link criteria
            * check_mapping - check and report if all external df recors are correctly linked
        * ComparedData - class for the result of comparison data from two source systems
            * create_compared - @classmethod for create a new object with data of comparison
* _mapping - collect classes representing different mapping data
    * AccMapping - mapping chart of account
    * EntityMapping -mapping entity structure
* _S4
    * DataS4 - class represents data from S4 
        * can create YTD file from monthly input files
        * can import YTD data
* _BPC
    * DataBPC - class represents data from BPC
        * can import BPM files (YTD only)
        * can compare BPC data with entity data and report differences (export in csv file, because file usually is too big to use pandas.to_excel) 
* _EPM
    * DataEPM - class represents data from EPM (BI system) 
        * can import EPM files (YTD only)
        * can compare EPM data with EPM data and report differences
* _static
    * collect all project static data
* df_manipulation
    * some technical functions to import and cleaning row data
* tools
    * StatusWraper - class define to wrap processes and reports status in StatusDF
    * StatusDF - datframe collection all statuses during processing data
    * some technical functions

## Technologies
* python 3.8
* numpy
* pandas 
* openpyxl

## Contact info
* project prepared by Piotr Kalista
* email: pjkalista@gmail.com

## Project Timeline
* 2022 Feb - create a base of project
* 2022 Mar-Jun - create jupyter notebooks to realise customer expected data 

## Project Future
* create reporting in HTML based on Jinja2
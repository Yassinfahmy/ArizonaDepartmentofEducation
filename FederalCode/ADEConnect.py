"""
Authors: ADE Accountability & Research

Last Updated: 07/19/2023

Description: This Python module contains a class (ADEConnect) which stores references to each of the Federal component classes 
    and which has methods for uploading the results of each component to the SQL server along with additional historical data.
"""

# import component modules
from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
from CA import CA
from DROPOUT import DROPOUT
from EL import EL
from GRADUATION import GRADUATION
from GROWTH import GROWTH
from proficiency import Proficiency
from TABLES import TABLES

# import third party libraries
import pandas as pd
import numpy as np
import traceback
import itertools
from termcolor import colored

"""

"""
class ADEConnect(COMPONENTS):
    def __init__(self, components_to_ignore:set = {}, fiscal_year:int=None, run:str='Prelim', **kwargs):
        """
        Parameters:
        ----------
        components_to_ignore (list): an iterable containing any of the following components: ["CA", "Dropout", "Growth", "Proficiency", "Graduation", "EL"].
            Whichever components are included will not be tracked by this object, meaning that the related results will be missing, shown as "." in SQL

        fiscal_year (int): The fiscal year of the results desired. This will impact the final SQL table names and column names

        run (str): A prefix that will appear in the SQL tables uploaded to the archive database

        kwargs (dict): optional keyword arguments to be passed to the superclass
        """
        # call COMPONENTS constructor
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)

        # class component names
        self.components = ["CA", "Dropout", "Growth", "Proficiency", "Graduation", "EL"]

        # map from class component names to component weights name
        self.component_weights_map = dict(zip(self.components, ["CA", "Dropout Rate", "Growth", "Proficiency", "Graduation Rate", "EL"]))

        # map from class component names to component names in the csi_summary SQL tables
        self.csi_summary_sql_cols_map = dict(zip(self.components, ["ChronicAbsenteeism", "Dropout", "Growth", "Proficiency", "Graduation", "EL"]))

        # map from class component names to component names in the atsi SQL tables
        self.atsi_sql_cols_map = dict(zip(self.components, ["CA", "DR", "Growth", "Proficiency", "GR", "EL"]))

        # map from class component names to component class instances
        self.component_classes = dict(zip(self.components, [CA(fiscal_year=self.fiscal_year, run=self.run, **kwargs), 
            DROPOUT(fiscal_year=self.fiscal_year, run=self.run, **kwargs), 
            GROWTH(fiscal_year=self.fiscal_year, run=self.run, **kwargs), 
            Proficiency(fiscal_year=self.fiscal_year, run=self.run, **kwargs),
            GRADUATION(fiscal_year=self.fiscal_year, run=self.run, **kwargs),
            EL(fiscal_year=self.fiscal_year, run=self.run, **kwargs)]))
        
        # map from class component names to csi drilldown column ordering for SQL tables
        self.drilldown_column_orders = dict(zip(self.components, [TABLES().get_csi_ca_drilldown_columns, 
            TABLES().get_csi_do_drilldown_columns, TABLES().get_csi_growth_drilldown_columns, 
            TABLES().get_csi_proficiency_drilldown_columns, TABLES().get_csi_gr_drilldown_columns, 
            TABLES().get_csi_el_drilldown_columns]))
        
        # maps from class component names to SQL archive table names
        self.archive_csi_drilldown_names = dict(zip(self.components, ["ChronicAbsenteeism", "DropoutRate", "Growth", "Proficient", "GradRate", "EL"]))
        self.archive_csi_summary_names = dict(zip(self.components, ["CSIChronicAbsenteeism", "CSIDropoutRate", "CSIGrowth", "CSIProficiency", "CSIGraduationRate", "CSIEL"]))
        self.archive_atsi_names = dict(zip(self.components, ["ATSIChronicAbsenteeism", "ATSIDropoutRate", "ATSIGrowth", "ATSIProficiency", "ATSIGraduationRate", "ATSIEL"]))
        self.csi_G_table_name = "SummaryCSIG"
        
        # maps from class component names to component results. The values are None until a method is called that calculates the results.
        self.csi_drilldown_results = dict(zip(self.components, [None]*6))
        self.csi_summary_results = dict(zip(self.components, [None]*6))
        self.atsi_results = dict(zip(self.components, [None]*6))
        self.csi_G = None # holds csi-G table

        # drop components that should be ignored
        self.components = [x for x in self.components if x not in components_to_ignore]

        # define database names
        self.archive_database = "AccountabilityArchive"
        self.target_database = "REDATA_UAT"

        # define map from SchoolTypeF to csi summary sql table name
        self.model_map = {1:"K2", 2:"KThru8", 3:"9Thru12", 4:"KThru11", 5:"KThru12"}

        # define historical csi and atsi thresholds
        self.csi_thresholds = {2022:21.99}
        self.tsi_thresholds = {2018:17.31471, 2019:18.9112147, 2022:11.27778105}


    """
    Uses each component class to calculate results and save them to class attributes
    """ 
    def calculate_results(self, static_file:pd.DataFrame=None, schooltype_data:pd.DataFrame=None, 
        dropout_data:pd.DataFrame=None, gradrates_data:pd.DataFrame=None, **kwargs):
        """
        Parameters:
        ----------
        static_file (pandas.DataFrame): an up-to-date version of the Static File published by ADE Accountability & Research 
            that contains records corresponding to this class's fiscal_year attribute

        schooltype (pandas.DataFrame): a dataframe containing information about all schools for which Accountability & Research reports Federal identification

        """
        if static_file is None: # download static file from SQL server if not provided
            static_file = DATABASE(fiscal_year = self.fiscal_year, run = self.run, schema = 'Static', 
                database = self.archive_database).read_table(table_name="StaticFile")
        
        # define database connection to get dropout and graduation rate data
        db = DATABASE(fiscal_year=self.fiscal_year, run=self.run, schema='Static', database='AccountabilityArchive')
        
        # gather data from Archive database if not provided in method arguments
        if (dropout_data is None) & ("Dropout" in self.components): 
            dropout_data = db.read_table(table_name ='DropOut')
        if (schooltype_data is None) & (any([x for x in ["Dropout", "Graduation"] if x in self.components])): 
            schooltype_data = db.read_table(table_name ='SchoolType')
        if (gradrates_data is None) & ("Graduation" in self.components): 
            gradrates_data = db.read_table(table_name ='GradRate')

        # define the arguments for the calculate_component() method from each component class. Currently using keyword arguments. Positional might be better at accomodating changes in component classes
        component_args = {
            "CA":{"staticfile":static_file},
            "Dropout":{"dropout":dropout_data, "schooltype":schooltype_data},
            "Growth":{"staticfile_raw":static_file},
            "Proficiency":{"static_file":static_file},
            "Graduation":{"gradrates":gradrates_data, "schooltypes":schooltype_data},
            "EL":{"staticfile":static_file}
        }

        # calculate each component's results
        for component in self.components:
            print(f"Calculating Federal {component} Results...")
            results = self.component_classes[component].calculate_component(**component_args[component]) # returns [csi summary, csi drilldown, atsi]
            results = self.round_numeric_cols(*results) # round all results
            self.csi_summary_results[component] = results[0] # save csi summary results to class

            # make some adjustments to the csi drilldown results
            drilldown_df = results[1]
            
            # customize drilldown results column order:
            # get desired order of the columns. This helps keep SQL organized for IT
            ordered_columns = self.drilldown_column_orders[component]() # call stored function
            
            # get a dataframe with all the cols from the target table thats missing from the source df
            missing_cols = [x for x in ordered_columns if x not in drilldown_df.columns]
            
            # print missing columns for the user to see
            if missing_cols:
                print(colored(f'****Warning****:\n{len(missing_cols)} columns in "{component}" drilldown results are missing:', 'red'))
                print(*missing_cols)
                print(colored('Columns will be filled with missing values.', 'red'))

                # fill missing columns
                drilldown_df[missing_cols] = np.nan
            
            # reorder columns according to order from TABLES
            drilldown_df = drilldown_df[ordered_columns]

            self.csi_drilldown_results[component] = drilldown_df # save re-ordered csi drilldown results to class

            self.atsi_results[component] = results[2] # save atsi results to class
        
        # calculate csi-G results
        if "Graduation" in self.components: # ignore if component is being ignored
            self.csi_G = self.component_classes["Graduation"].calculate_csi_G(gradrates_data, schooltype_data, **kwargs)

    """
    Uploads component results to the SQL archive database
    """
    def upload_results_to_db(self, database:str=None, schema:str="Results", run:str=None):
        """
        Parametes:
        ----------
        database (str): The name of the SQL database where the component results are uploaded. As of 07/19/2023, the default database is AccountabilityArchive

        schema (str): The schema to use when uploading results. Default is "Results"

        run (str): A prefix for the SQL table names given to the results when uploaded. Default is the run attribute of the class.
        """
        # check if results have been assigned. If not, calculate them using tables in AccountabilityArchive database
        for x in self.components:
            if not all([False if y is None else True for y in [self.csi_drilldown_results[x], self.csi_summary_results[x], self.atsi_results[x]]]):
                print("Calculating Results...")
                self.calculate_results()
                break
            
        # establish connection to AccountabilityArchive database
        if database is None: database = self.archive_database
        if run is None: run = self.run
        db = DATABASE(fiscal_year=self.fiscal_year, run=run, schema=schema, database=database)

        # upload component results
        for component in self.components:
            # delete existing tables
            db.drop_tables_in_run(self.archive_csi_drilldown_names[component], self.archive_csi_summary_names[component], self.archive_atsi_names[component], table_prefix=db.run)

            # upload csi drilldown table, csi summary table, and atsi table
            db.upload_table_to_db(df=self.csi_drilldown_results[component], table_name=self.archive_csi_drilldown_names[component], all_nvarchar=True)
            db.upload_table_to_db(df=self.csi_summary_results[component], table_name=self.archive_csi_summary_names[component])
            db.upload_table_to_db(df=self.atsi_results[component], table_name=self.archive_atsi_names[component])
            
        # upload csi-G results
        if "Graduation" in self.components: # ignore if component is being ignored
            db.drop_tables_in_run(self.csi_G_table_name, table_prefix=db.run)
            db.upload_table_to_db(df=self.csi_G, table_name=self.csi_G_table_name)

            
    """
    Loads component results from the SQL archive database and stores them as class attributes. This method can be used instead of calculate_results() 
        if the results in the archive database are up-to-date
    """
    def retreive_results(self, database:str=None, schema:str="Results", run:str=None):
        """
        Parameters:
        ----------
        database (str): An optional argument to specify the database from which to retrieve component results. Default is to use the archive database class attribute.

        schema (str): An optional argument to specify the database schema for the component results. Default is "Results"
        """
        # establish connection to archive database where results should be available
        if database is None: database = self.archive_database
        if run is None: run = self.run
        db = DATABASE(fiscal_year=self.fiscal_year, run=self.run, schema=schema, database=database)

        # download component results
        for component in self.components:
            self.csi_summary_results[component] = db.read_table(table_name=self.archive_csi_summary_names[component])
            self.atsi_results[component] = db.read_table(table_name=self.archive_atsi_names[component])
            self.csi_drilldown_results[component] = db.read_table(table_name=self.archive_csi_drilldown_names[component])

        # download csi-G results
        if "Graduation" in self.components: # ignore if component is being ignored
            self.csi_G = db.read_table(df=self.csi_G, table_name=self.csi_G_table_name)
            

    """
    Uploads class csi drilldown results to the final SQL tables that will fill GUIS on ADEConnect
    """
    def upload_csi_drilldowns(self, database:str=None, schema:str="ssi", run:str=""):
        """
        Parameters:
        -----------
        database (str): An optional argument that specifies which databse the results are uploaded to. Default is the target database class attribute.

        schema (str): An option argument that specifies which schema to upload the results to. Default value is "ssi".

        run (str): An optional prefix of the SQL table to which the results will be uploaded. Default value is "", so no prefix.
        """
        # calculate all component results if csi drilldown results do not already exist
        for x in self.components:
            if self.csi_drilldown_results[x] is None:
                print("Calculating Results...")
                self.calculate_results()
                break

        # define database connection for uploading csi drilldown tables
        if database is None: database = self.target_database
        db = DATABASE(self.fiscal_year, run=run, schema=schema, database=database)
        for component in self.components:
            df_model = self.csi_drilldown_results[component].copy() # copy result to avoid overwriting

            # define table name of SQL GUI results table
            # table_name = f"{db.database}.{db.schema}.{db.run}{}{db.fiscal_year}"

            # round results to two decimal places
            df_model = self.round_numeric_cols(df_model)[0]

            # delete existing tables
            # db.drop_tables_in_run(self.archive_csi_drilldown_names[component], table_prefix=db.run)

            # upload dataframe to existing table in sql server. Make sure to empty target table first.
            db.fill_table(df_model, table_name=self.archive_csi_drilldown_names[component], clear_table=True, all_data=False, create_if_not_exist=True, all_nvarchar=True)

    """
    Combines the csi summary results produced by each component into a single dataframe containing all csi summary results
    """
    def combine_csi_summary(self, calculate_total_points:bool=True, append_historical_results:bool=True, identification_year:bool=False,
        get_threshold_mean_std:bool=False, identify_schools:bool=True, **kwargs):
        """
        Parameters:
        ----------
        calculate_total_points (bool): Determines whether total points are calculated (and included in the returned dataframe) 
            for each school based on the CSI summary results from each component

        append_historical_results (bool): Determines whether historical CSI summary results are collected from SQL and included in the returned dataframe

        kwargs (dict): optional keyword arguments given to get_csi_historical()

        Returns:
        ----------
        pandas.DataFrame: a single dataframe containing all of the CSI summary results for each component and federal model. 
            May or may not contain total point values depending on the method parameters provided.
        """
        # calculate component results if they are not stored
        for x in self.components:
            if self.csi_summary_results[x] is None:
                print("Calculating Results...")
                self.calculate_results()
                break
        
        # form a list of each component's csi summary results with index as EntityID and SchoolTypeF
        csi_summary_results = []
        for component in self.components:
            csi_summary_results.append(self.csi_summary_results[component].set_index(["EntityID", "SchoolTypeF"]))

        # merge all csi results tables together on EntityID and SchoolTypeF
        csi_summary_results = pd.concat(csi_summary_results, axis=1).reset_index()

        # calculate and append total csi summary points
        if calculate_total_points:
            csi_total_points = self.csi_total_points(combined_csi_summary=csi_summary_results)
            csi_total_points[f"OverallPoints{self.fiscal_year}"] = csi_total_points["TotalPointsEarned"]
            csi_summary_results = pd.merge(csi_summary_results, csi_total_points, on=["EntityID", "SchoolTypeF"], how="outer")
        if identify_schools:
            # get title 1 information and previous identifications
            school_info = self.get_federal_identifications() # TODO
            title_1_mask = csi_summary_results["EntityID"].isin(school_info[school_info["IsTitle1"]==1]["EntityID"])
            if get_threshold_mean_std: # getting cutoffs for atsi/tsi
                # calculate csi-LA 5% threshold
                threshold = self._calculate_csi_threshold(csi_summary_results, title_1_mask)

                # calculate CAS mean and std from all schools
                eligible_schools = csi_summary_results[~(csi_summary_results["TotalPointsEarned"].isna())] # do not include schools that were eligible for zero points
                cas_mean = eligible_schools["TotalPointsEarned"].mean()
                cas_std = eligible_schools["TotalPointsEarned"].std()
                return threshold, cas_mean, cas_std

            # identify or exit schools for csi
            if identification_year: 
            ### calculate bottom 5% threshold and identify schools if list of title_1_schools is provided ###
                threshold = self._calculate_csi_threshold(csi_summary_results, title_1_mask)
                below_threshold = csi_summary_results["TotalPointsEarned"]<threshold
                csi_summary_results.loc[title_1_mask, "Bottom5PctThreshhold"] = threshold
                csi_summary_results.loc[(title_1_mask), "IdentifiedforCSILowAchievement"] = np.select(
                    condlist=[(title_1_mask & below_threshold), (title_1_mask & ~below_threshold)], choicelist=["Yes", "No"], default=np.nan
                )
                
            else: # pull last year's identifications and threshold and use them to exit schools
                previously_identified = school_info[school_info["Identified"]==1]["EntityID"]
                previously_identified = csi_summary_results["EntityID"].isin(previously_identified)
                below_threshold = csi_summary_results["TotalPointsEarned"]<self.csi_thresholds[self.previous_fiscal_year]

                # identify schools that have not exited
                csi_summary_results.loc[title_1_mask, "Bottom5PctThreshhold"] = self.csi_thresholds[self.previous_fiscal_year]
                csi_summary_results.loc[(title_1_mask), "IdentifiedforCSILowAchievement"] = np.select(
                    condlist=[(previously_identified & below_threshold), (title_1_mask & ~(previously_identified & below_threshold))], choicelist=["Yes", "No"], default=np.nan
                )
        else:
            csi_summary_results.loc[:,["IdentifiedforCSILowAchievement", "Bottom5PctThreshhold"]] = np.nan # default value when not identifying schools for csi

        # obtain historical results from database and append to csi summary results
        if append_historical_results:
            csi_historical = self.get_csi_historical(**kwargs)
            csi_summary_results = pd.merge(csi_summary_results, csi_historical, on=["EntityID", "SchoolTypeF"], how="outer")

        csi_summary_results["FiscalYear"] = self.fiscal_year # add fiscal year column
        
        return csi_summary_results

    """
    Calculates the total CSI points earned, total CSI points eligible, and normalized total CSI points earned for each school based on their Federal Model
    """
    def csi_total_points(self, combined_csi_summary:pd.DataFrame=None):
        """
        Parameters: 
        ----------
        combined_csi_summary (pandas.DataFrame): a dataframe containing all of the csi point values for each component. If not provided, this will be 
            created from class csi summary component results.

        Returns:
        ----------
        pandas.DataFrame: a dataframe containing the total csi point columns for each school and, optionally, all csi summary results depending 
            on the method parameters given

        """
        # combine csi summary results if not provided
        if combined_csi_summary is None:
            combined_csi_summary = self.combine_csi_summary(calculate_total_points=False, append_historical_results=False)

        component_cols_used = [self.csi_summary_sql_cols_map[x] for x in self.components]
        component_weights_used = [self.component_weights_map[x] for x in self.components]

        # calculate the raw total points earned by each school
        total_points_data = combined_csi_summary[["EntityID", "SchoolTypeF"] + component_cols_used].replace({np.nan:0}).apply(pd.to_numeric, errors='ignore')
        total_points_data["AllStudentsTotalPoints"] = total_points_data[component_cols_used].sum(axis=1)

        # calculate the number of points that were eligible for each school #
        # pull out point columns and rename to match model weights in superclass
        points_eligible = combined_csi_summary[["EntityID", "SchoolTypeF"] + component_cols_used]

        # loop through the different kinds of federal school models
        for model in self.federal_model_weights.keys():
            # get all schools for a given model
            temp = points_eligible[points_eligible["SchoolTypeF"]==model][component_cols_used]

            # replace component point values with federal model weights for given model or zero if a model doesn't use a component
            temp.loc[:,:] = np.repeat(np.array([self.federal_model_weights[model][x] if x in self.federal_model_weights[model].keys() 
                else 0 for x in component_weights_used])[None,:], temp.shape[0], axis=0)
            points_eligible.loc[points_eligible["SchoolTypeF"]==model, component_cols_used] = temp.values

        # assign zero where schools did not meet component eligibility-requirements
        temp_df = points_eligible[component_cols_used]
        temp_df[combined_csi_summary[component_cols_used].isna()] = 0
        points_eligible.loc[:, component_cols_used] = temp_df.values

        # calculate total points eligible for each school-model by summing over components
        points_eligible["TotalPointsEligible"] = points_eligible.drop(columns=["EntityID", "SchoolTypeF"]).sum(axis=1)

        # append total points eligible to total points earned
        total_points_data = pd.merge(total_points_data, points_eligible[["EntityID", "SchoolTypeF", "TotalPointsEligible"]], on=["EntityID", "SchoolTypeF"], how="outer")

        # normalize the total points earned so that each school's score is out of 100
        total_points_data["TotalPointsEarned"] = np.select(
            condlist=[total_points_data["AllStudentsTotalPoints"].notna(), total_points_data["AllStudentsTotalPoints"].isna()], 
            choicelist=[total_points_data["AllStudentsTotalPoints"]/total_points_data["TotalPointsEligible"] * 100, np.nan]
        )
        total_points_data["TotalPointsEarned"] = total_points_data["AllStudentsTotalPoints"]/total_points_data["TotalPointsEligible"] * 100

        return total_points_data[["EntityID", "SchoolTypeF", "AllStudentsTotalPoints", "TotalPointsEligible", "TotalPointsEarned"]]



    """
    Reads in each Federal Model's CSI result SQL table from the previous fiscal year, combines them on EntityID, and extracts all of the historical 
        (containing a year in the name) columns.
    """
    def get_csi_historical(self, database:str = None, schema:str = "ssi", run:str = "Summary", **kwargs):
        """
        Parameters:
        ----------
        database (str): The database in which the historical CSI summary tables are stored

        schema (str): The schema used to denote the historical CSI summary tables

        run (str): A common prefix used in the name of each historical CSI summary table in the SQL database

        Returns:
        ----------
        pd.DataFrame: A dataframe containing all of the CSI points for each component earned over the previous three years, 
            concatentated together using the EntityID of each school
        """
        # establish connection to sql server
        if database is None: database = self.target_database
        db = DATABASE(fiscal_year=self.previous_fiscal_year, run=run, schema=schema, database=database)

        # read in the historical csi summary table for each Federal model
        historic_csi = []
        for model, table_name in self.model_map.items():
            df = db.read_table(table_name=table_name)
            df["SchoolTypeF"] = model
            historic_csi.append(df.set_index(["EntityID", "SchoolTypeF"]))
        historic_csi = pd.concat(historic_csi, axis=0)

        # pull out historical columns
        regex = []
        for i in range(self.fiscal_year-3, self.fiscal_year):
            regex.append(f'Points{str(i)}') # gets columns like Points____, where ____ is a year in the range defined above
        regex = "|".join(regex)
        cols = historic_csi.columns[historic_csi.columns.str.contains(regex, regex=True)]
        historic_csi = historic_csi.loc[:, cols].reset_index() # extract historical columns
        historic_csi = historic_csi.apply(pd.to_numeric, errors='coerce') # convert to numeric
        return historic_csi



    """
    Uploads combined csi summary results (with total points) to the target SQL database
    """
    def upload_csi_summaries(self, csi_summary_results:pd.DataFrame=None, database:str=None, schema:str="ssi", 
        run:str="", identify_schools=False, identification_year=False, **kwargs):
        """
        Parameters:
        ----------
        csi_summary_results (pandas.DataFrame): Pre-calculated and combined csi summary results, like the value returned by combine_csi_summary()

        database (str): The name of the database to which the csi summary results are uploaded. Default is to use the class target database.

        schema (str): A schema for the tables to which the csi summary results are uploaded. Default value is "ssi".

        run(str): A prefix for the tables two which the csi summary results are uploaded. Default value is "", so no prefix.
        """
        # combine csi summary results if not provided
        if csi_summary_results is None:
            csi_summary_results = self.combine_csi_summary(identify_schools=identify_schools, identification_year=identification_year, **kwargs)

        # define database connection for uploading csi summary tables
        if database is None: database = self.target_database
        db = DATABASE(self.fiscal_year, run=run, schema=schema, database=database)
        for model_number, model_string in self.model_map.items():
            # get relevant data from combined csi summary data
            df_model = csi_summary_results[csi_summary_results["SchoolTypeF"]==model_number]

            # specify columns that we do not fill
            columns_to_ignore = [f"{db.run}Summary{model_string}{db.fiscal_year}Key", "CreatedBy", "CreatedDate", "LastModifiedBy", "LastModifiedDate"]

            # round results to two decimal places
            df_model = self.round_numeric_cols(df_model)[0]

            # upload dataframe to existing table in sql server. Make sure to empty target table first.
            db.fill_table(df=df_model, table_name=f"Summary{model_string}", clear_table=True, all_data=False, cols_to_ignore=columns_to_ignore)


    """
    Combines the atsi results produced by each component into a single dataframe containing all of the atsi results
    """
    def combine_atsi_results(self, calculate_total_points:bool=True, append_historical_results:bool=True, identification_year:bool=True, 
        identify_schools:bool=True, **kwargs):
        """
        Parameters:
        ----------
        calculate_total_points (bool): Determines whether the total points earned and eligible and calculated and appended to the returned 
            combined results. Default is True, so total points are calculated and appended to the returned dataframe.

        append_historical_results (bool): Determines whether historical atsi results are appended to the returned combined results. 
            Default is True, so the historical results are appended to the returned dataframe.

        Returns:
        ----------
        pandas.DataFrame: The combined atsi results of each component, plus total points and/or historical results 
            depending on the method parameters provided.
        """
        # calculate component results if they are not yet stored
        for x in self.components:
            if self.atsi_results[x] is None:
                print("Calculating Results...")
                self.calculate_results()
                break

        # form a list of each component's atsi summary results with index as EntityID and federal model
        atsi_results = []
        for component in self.components:
            atsi_results.append(self.atsi_results[component].set_index(["EntityID", "SchoolTypeF"]))

        # merge all atsi results tables together on EntityID and SchoolTypeF
        atsi_results = pd.concat(atsi_results, axis=1).reset_index()

        # calculate and append total atsi summary points
        if calculate_total_points:
            total_points = self.atsi_total_points(combined_atsi=atsi_results)
            atsi_results = pd.merge(total_points, atsi_results, on=["EntityID", "SchoolTypeF"], how="outer")
            for subgroup in [x for x in self.db_subgroups.values() if x != "All"]: # add additional columns for current fiscal year points earned
                atsi_results[f"{subgroup}{self.fiscal_year}"] = atsi_results[f"TPEarned{subgroup}"]

        if append_historical_results or identify_schools:
            atsi_historical = self.get_atsi_historical(**kwargs)
            atsi_results = pd.merge(atsi_results, atsi_historical, on=["EntityID"], how="outer")
        
        # HERE WORKING ON ATSI/TSI IDENTIFICATION
        if identify_schools: # add identification and on-watch columns for each subgroup
            # Identify ATSI #
            # calculate csi-LA 5% threshold
            # csi_threshold = self.combine_csi_summary(title_1_schools=title_1_schools, calculate_threshold=True)

            # merge atsi results with Proficiency eligibility of each school-subgroup. Only Proficiency-eligible school-subgroups are eligible for identification
            # atsi_results = atsi_results.merge(eligible_school_subgroups, on=["EntityID", "Subgroup"], how="left")

            # identify eligible subgroups based on csi-LA threshold
            # for subgroup in self.db_subgroups.values(): # add additional columns for current fiscal year points earned
            #     eligible_identified = ((atsi_results[f"TPEarned{subgroup}"] < csi_threshold) & (atsi_results[f"Proficiency{subgroup}"].isna()))
            #     eligible_not_identified = ((atsi_results[f"TPEarned{subgroup}"] >= csi_threshold) & (atsi_results["Eligible"] == 1))
            #     atsi_results[f"aTSI{subgroup}"] = np.select(condlist=[eligible_identified, eligible_not_identified], 
            #         choicelist=[1, 0], default = np.nan)
                
            # What should the identification columns above contain during non-identification years? Everything? Nothing? Who exited?
            
            # Identify TSI #

            for subgroup in [x for x in self.db_subgroups.values() if x != "All"]:
                atsi_results[f"{subgroup}IdentifiedTSI"] = np.nan # STILL NEED TO DO THIS
                atsi_results[f"{subgroup}OWTSI"] = np.nan # STILL NEED TO DO THIS

            atsi_results[f"TwoSDTarget{self.fiscal_year}"] = np.nan # STILL NEED TO DO THIS
            atsi_results["TwoSDTargetIdentifiedTSI"] = np.nan # empty column
            atsi_results["TwoSDTargetOWTSI"] = np.nan # empty column
        else: # default path when not identifying schools for atsi or tsi
            for subgroup in self.db_subgroups.values():
                atsi_results.loc[:,[f"aTSI{subgroup}", f"{subgroup}IdentifiedTSI", f"{subgroup}OWTSI"]] = np.nan
            atsi_results.loc[:,[f"TwoSDTarget{self.fiscal_year}", "TwoSDTargetIdentifiedTSI", "TwoSDTargetOWTSI"]] = np.nan

        atsi_results["FiscalYear"] = self.fiscal_year

        
        return atsi_results
    

    """
    Calculates the total points earned, total points eligible, and normalized total points earned from the atsi results of each component. 
    """
    def atsi_total_points(self, combined_atsi:pd.DataFrame=None, eligible_for_proficiency:list=None):
        """
        Parameters:
        ----------
        combined_atsi (pandas.DataFrame): The combined atsi results of each component, like the value returned by combine_atsi_results()

        Returns:
        ----------
        pandas.DataFrame: A dataframe containing the total atsi points earned and eligible for each school-subgroup, plus the combined 
            atsi results for each component depending on the parameters provided.
        """
        # combine atsi results if not provided
        if combined_atsi is None:
            combined_atsi = self.combine_atsi_results(calculate_total_points=False, append_historical_results=False)

        atsi_point_cols_used = [self.atsi_sql_cols_map[x] for x in self.components]
        component_weights_used = [self.component_weights_map[x] for x in self.components]

        # pull out all of the points-related columns, using previous table as a guide for column names.
        subgroups = list(self.db_subgroups.values())
        point_columns = ["".join(x) for x in list(itertools.product(atsi_point_cols_used, subgroups))]
        atsi_points_data = combined_atsi.set_index(["EntityID", "SchoolTypeF"])[list(point_columns)]

        # separate column names into two parts: component and subgroup.
        columns = atsi_points_data.columns.to_series().replace({x:x+";" for x in atsi_point_cols_used}, regex=True)
        columns = columns.str.split(pat=";", n=1, expand=True).replace({";":""}, regex=True).rename(columns={0:"Component", 1:"Subgroup"})

        # Make into two column levels, melt (stack) the subgroup level of columns into a single column. Now index is (EntityID, SchoolTypeF, Subgroup) and columns are components
        atsi_points_data.columns = pd.MultiIndex.from_frame(columns)
        atsi_points_data = atsi_points_data.stack(level="Subgroup")

        # reset the index back to columns
        atsi_points_data = atsi_points_data.reset_index()

        # HERE get a list of school-subgroups that are eligible for proficiency:
        if eligible_for_proficiency:
            eligible_school_subgroups = atsi_points_data[["EntityID", "Subgroup"]].copy()
            atsi_points_data.loc[~atsi_points_data["Proficiency"].isna(), "Eligible"] = 1

        ### calculate the raw total points earned by each school-model-subgroup ###
        # replace missing points (ineligible) with zero
        total_points_data = atsi_points_data.copy().replace({np.nan:0}).apply(pd.to_numeric, errors='ignore')

        # sum over components to get the total points earned by each school-model-subgroup
        total_points_data["TotalPoints"] = total_points_data.drop(columns=["EntityID", "SchoolTypeF", "Subgroup"]).sum(axis=1)

        ### calculate the number of points for which each school-model-subgroup is eligible ###
        # rename some columns to match component names in code elsewhere
        points_eligible = atsi_points_data.copy().apply(pd.to_numeric, errors='ignore')

        # assign component weights based on federal model type
        for model in self.federal_model_weights.keys():
            # select all school-subgroup pairs for a federal model type
            temp_df = points_eligible[points_eligible["SchoolTypeF"]==model][atsi_point_cols_used]

            # replace component point values with federal model weights for given model or zero if a model doesn't use a component
            temp_df.loc[:,:] = np.repeat(np.array([self.federal_model_weights[model][x] if x in self.federal_model_weights[model].keys() 
                else 0 for x in component_weights_used])[None,:], temp_df.shape[0], axis=0)
            
            points_eligible.loc[points_eligible["SchoolTypeF"]==model, atsi_point_cols_used] = temp_df.values

        # assign zero where schools did not meet component eligibility-requirements
        temp_df = points_eligible[atsi_point_cols_used]
        temp_df[atsi_points_data[atsi_point_cols_used].isna()] = 0
        points_eligible.loc[:, atsi_point_cols_used] = temp_df.values

        # sum over components to get eligible points for each school-subgroup
        points_eligible["TPEligible"] = points_eligible.drop(columns=["EntityID", "SchoolTypeF", "Subgroup"]).sum(axis=1)

        # append total points eligible to total points
        total_points_data = pd.merge(total_points_data, points_eligible[["EntityID", "SchoolTypeF", "Subgroup", "TPEligible"]], on=["EntityID", "SchoolTypeF", "Subgroup"], how="outer")

        # normalize the total points earned so that each school-subgroup's score is out of 100
        total_points_data["TPEarned"] = np.select(
            condlist=[total_points_data["TotalPoints"].notna(), total_points_data["TotalPoints"].isna()], 
            choicelist=[total_points_data["TotalPoints"]/total_points_data["TPEligible"] * 100, np.nan]
        )

        # convert back to wide form (subgroup to columns)
        total_points_data = total_points_data.pivot(columns=["Subgroup"], index=["EntityID", "SchoolTypeF"]).reset_index()
        columns = total_points_data.columns.to_frame()
        columns = columns.iloc[:,0] + columns.iloc[:,1]
        total_points_data.columns = columns

        return total_points_data[["EntityID", "SchoolTypeF"] + list(total_points_data.columns[total_points_data.columns.str.contains(
            "TotalPoints|TPEligible|TPEarned", regex=True)])]
        # if eligible_for_proficiency: 
        #     return df_to_return, eligible_school_subgroups
        # else: return df_to_return



    """
    Reads in the two ATSI result SQL tables from the previous fiscal year, combines them on EntityID, and extracts all of the historical 
        (containing a year in the name) columns.
    """
    def get_atsi_historical(self, database:str=None, schema:str = "Results", run:str = "Final"):
        """
        Parameters:
        ----------

        Returns:
        ----------
        pandas.DataFrame: A dataframe containing historical atsi results of each component (columns) for each school-subgroup (rows) over the past 4 years.
        """
        # establish connection to sql server
        if database is None: database = self.archive_database
        db = DATABASE(fiscal_year=self.previous_fiscal_year, run=run, schema=schema, database=database)
        try: # combine historical ATSI tables
            historic_atsi = []
            for table_name in self.py_atsi_tables.values():
                historic_atsi.append(db.read_table(table_name=table_name).set_index(["EntityID"]))
            historic_atsi = pd.concat(historic_atsi, axis=1)
        except Exception:
            traceback.print_exc()

        # pull out historical columns
        regex = []
        for i in range(self.fiscal_year-5, self.fiscal_year-1):
            regex.append(f'Y{str(i)}') # gets FY____ and CY____ columns, where ____ is a year in the range defined above
        regex.append(f'FY{str(self.fiscal_year-1)}') # used to get previous year's results for each component besides graduation (which has a year lag) 
        regex = "|".join(regex)
        cols = historic_atsi.columns[historic_atsi.columns.str.contains(regex, regex=True)]

        # temporary addition (for 2023):
        cols = list(cols) + ["TPEarnedWhite", "TPEarnedAfricanAmerican", "TPEarnedHispanicLatino", 
            "TPEarnedAsian", "TPEarnedNativeAmerican", "TPEarnedPacificIslander", "TPEarnedTwoorMoreRaces", 
            "TPEarnedELFEP14", "TPEarnedSWD", "TPEarnedIE12"]

        historic_atsi = historic_atsi.loc[:, cols].reset_index() # extract historical columns. This line should stay in 2024
        columns = historic_atsi.columns.to_series()
        columns = columns.replace({"TPEarnedWhite":f"White{self.fiscal_year-1}", "TPEarnedAfricanAmerican":f"AfricanAmerican{self.fiscal_year-1}", 
            "TPEarnedHispanicLatino":f"HispanicLatino{self.fiscal_year-1}", "TPEarnedAsian":f"Asian{self.fiscal_year-1}", 
            "TPEarnedNativeAmerican":f"NativeAmerican{self.fiscal_year-1}", "TPEarnedPacificIslander":f"PacificIslander{self.fiscal_year-1}", 
            "TPEarnedTwoorMoreRaces":f"TwoorMoreRaces{self.fiscal_year-1}", "TPEarnedELFEP14":f"ELFEP14{self.fiscal_year-1}", 
            "TPEarnedSWD":f"SWD{self.fiscal_year-1}", "TPEarnedIE12":f"IE12{self.fiscal_year-1}"})
        historic_atsi.columns = columns
        
        historic_atsi["TwoSDTarget2022"] = np.nan # STILL NEED TO DO THIS
        
        # temporary way to get 2019 points earned from excel file (wasn't previously tracked): 
        path_to_file = r"\\Asasprdvm01\acct\FED ACCOUNTABILITY\Results19\total_ATSI_TSI_2019_5year.xlsx"
        atsi_history_2019 = pd.read_excel(path_to_file, sheet_name=None)
        excel_to_subgroup = {"Asian":"Asian", "Black":"AfricanAmerican", "ELL":"ELFEP14", "FRL":"IE12", "Hispanic":"HispanicLatino", "Indian":"NativeAmerican", 
            "Pacific":"PacificIslander", "Multiple":"TwoorMoreRaces", "SPED":"SWD", "White":"White"}
        atsi_points_2019 = []
        for sheet in excel_to_subgroup.keys():
            atsi_points_2019.append(atsi_history_2019[sheet].rename(columns={"schoolid":"EntityID", "TotalPoints_Rate":"TPEarned"})[["EntityID", "TPEarned"]].rename(
                columns={"TPEarned":"{}{}".format(excel_to_subgroup[sheet], 2019)}).set_index(["EntityID"]))
        atsi_points_2019 = pd.concat(atsi_points_2019, axis=1).reset_index()

        atsi_points_2019["TwoSDTarget2019"] = np.nan # STILL NEED TO DO THIS

        # add 2019 points earned to historical data
        historic_atsi = pd.merge(historic_atsi, atsi_points_2019, on=["EntityID"], how="left")

        historic_atsi = historic_atsi.replace({"%":""}, regex=True).apply(pd.to_numeric, errors='coerce') # convert to numeric. This line should stay in 2024


        # Next year (2024), will want to revise this method to get historical points columns from SummaryATSI (new table as of 2023)

        return historic_atsi
    


    """
    Uploads atsi results, including total points earned/eligible and historical results, to a final target table in the SQL server.
    """
    def upload_atsi(self, atsi_results:pd.DataFrame=None, database:str=None, schema:str="ssi", run:str="", identify_schools:bool=True, identification_year:bool=False, **kwargs):
        """
        atsi_results (pandas.DataFrame): 
        """
        # combine atsi results if not provided
        if atsi_results is None:
            atsi_results = self.combine_atsi_results(identify_schools=identify_schools, identification_year=identification_year, **kwargs)

        # round results to two decimal places
        atsi_results = self.round_numeric_cols(atsi_results)[0]

        # define connection to sql database that will be used to upload tables
        if database is None: database = self.target_database
        db = DATABASE(self.fiscal_year, run=run, schema = schema, database=database)

        ### upload [SummaryATSI] ###
        # specify columns that we do not fill
        columns_to_ignore = [f"{db.run}SummaryATSI{db.fiscal_year}Key", "CreatedBy", "CreatedDate", "LastModifiedBy", "LastModifiedDate"]
        db.fill_table(df=atsi_results, table_name="SummaryATSI", cols_to_ignore=columns_to_ignore)

         ### upload [ProfATSI] ###
        # specify columns that we do not fill
        columns_to_ignore = [f"{db.run}ProfATSI{db.fiscal_year}Key", "CreatedBy", "CreatedDate", "LastModifiedBy", "LastModifiedDate"]
        db.fill_table(df=atsi_results, table_name="ProfATSI", cols_to_ignore=columns_to_ignore)

        ### upload [FinalGrowthCAGRDRATSI] ###
        # specify columns that we do not fill
        columns_to_ignore = [f"{db.run}GrowthCAGRDRATSI{db.fiscal_year}Key", "CreatedBy", "CreatedDate", "LastModifiedBy", "LastModifiedDate"]
        db.fill_table(df=atsi_results, table_name="GrowthCAGRDRATSI", cols_to_ignore=columns_to_ignore)

    """
    
    """
    def upload_csi_G(self, database:str = None, schema:str = "ssi", run:str = "", **kwargs):
        if self.csi_G is None: self.calculate_results() # calculate results if not calculated or retrieved previously

        if database is None: database = self.target_database
        db = DATABASE(self.fiscal_year, run=run, schema = schema, database=database) # connection to database

        # specify columns we do not fill
        columns_to_ignore = [f"{db.run}{self.csi_G_table_name}{db.fiscal_year}Key", "CreatedBy", "CreatedDate", "LastModifiedBy", "LastModifiedDate"]
        db.fill_table(df=self.csi_G, table_name=self.csi_G_table_name, cols_to_ignore=columns_to_ignore)

       

        
    """
    Uploads results (csi drilldown, csi summary, and atsi) to the final SQL tables in the database used by IT
    """
    def fill_federal_tables(self, use_archive_results:bool=False, static_file:pd.DataFrame=None, 
        csi_drilldown:bool=True, csi_summary:bool=True, atsi:bool=True, csi_g:bool=True, database:str=None, schema:str="ssi", 
        run:str="", **kwargs):
        """
        Parameters:
        ----------
        use_archive_results (bool): determines whether to use the component results stored in the archive database. Default is False and 
            component results are recalculated.

        static_file (pandas.DataFrame): A version of the static file that can be used by each component class to calculate results. Default 
            is None and the static file is downloaded from the archive database.

        csi_drilldown (bool): Determines whether to upload csi drilldown results to the final SQL tables. Default is True and csi drilldown 
            results are uploaded.

        csi_summary (bool): Determines whether to upload csi summary results to the final SQL tables. Default is True and csi summary results 
            are uploaded.

        atsi (bool): Determines whether to upload atsi results to the final SQL tables. Default is True and atsi results are uploaded.

        csi_g (bool): Determines whether to upload csi-G results to the final SQL tables. Default is True and the csi-G results are uploaded. 

        database (str): The name of the database to which the results should be uploaded. This is the databse in which IT expects the final 
            results to be. Default value is the target database attribute of the class.

        schema (str): A schema associated with the final SQL tables to which the results are being uploaded. Default value is "ssi".

        run (str): A prefix for the target SQL tables. Default value is "", so no prefix.
        """

        # get results if not already done
        for x in self.components:
            if None in [self.csi_drilldown_results[x], self.csi_summary_results[x], self.atsi_results[x]]:
                print("Calculating Results...")
                if use_archive_results: self.retreive_results() # use results already in SQL
                else: self.calculate_results(static_file=static_file) # calculate new results
                break    

        if csi_drilldown:
            # upload csi drilldown results
            self.upload_csi_drilldowns(database=database, schema=schema, run=run)
        
        if csi_summary:
            # upload csi summary tables
            self.upload_csi_summaries(**kwargs, database=database, schema=schema, run=run)

        if atsi:
            # upload atsi tables
            self.upload_atsi(**kwargs, database=database, schema=schema, run=run)

        if csi_g:
            # upload csi low graduation table
            self.upload_csi_G(**kwargs, database=database, schema=schema, run=run)

    
    # returns lists of schools that were identified for csi and for atsi in the previous fiscal year
    def get_identified(self, csi:bool=True, atsi:bool=True, database:str=None, schema:str="grading", run:str=""):
        # define database connection
        if database is None: database = self.target_database
        db = DATABASE(fiscal_year=self.previous_fiscal_year, database=database, schema=schema, run=run)
        df = db.read_table(table_name=f"AThruFEntityData{db.fiscal_year}")
        # get list of schools that were identified for csi last year
        if csi: csi_schools = df[(df["FiscalYear"]==self.previous_fiscal_year) & (df["CSI-LowAchievement"].notna())]["EntityID"].to_list()
        else: csi_schools = None
        if atsi: atsi_schools = df[(df["FiscalYear"]==self.previous_fiscal_year) & (df["TSI"].notna())]["EntityID"].to_list()
        else: atsi_schools = None
        return csi_schools, atsi_schools
    

    # not using this after all
    def get_title_I_schools(self, database="Accountability", schema="EdOrg", run=""):
        db = DATABASE(fiscal_year=self.previous_fiscal_year, database=database, schema=schema)
        # schools = db.read_table(table_name="School", columns="SchoolCode" filter=f"FiscalYear={db.fiscal_year} AND Title1SchoolStatus='Funded'")[""]

    # upload a table to the sql server containing title 1 status and previous identification status
    def upload_federal_identifications(self, title_1_info_path:str, identification_info_path:str):
        pass

    # return table with title 1 info and previous identifications
    def get_federal_identifications(self) -> pd.DataFrame:pass


    def _calculate_csi_threshold(csi_summary_results:pd.DataFrame, title_1_mask:pd.DataFrame):
        proficiency_eligible_mask = ~(csi_summary_results["Proficiency"].isna()) # use schools that were eligible for Proficiency
        eligible_schools = csi_summary_results[(proficiency_eligible_mask) & (title_1_mask)]
        threshold = np.percentile(eligible_schools["TotalPointsEarned"].values, q=5, interpolation="midpoint")
        return threshold












    


    


    
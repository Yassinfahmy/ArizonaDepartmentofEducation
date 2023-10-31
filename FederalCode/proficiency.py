"""
Created by: ADE Accountability & Research

Last Updated: 04/28/2023

Description: This Python module contains a Proficiency class capable of producing the results from the Proficiency component of the Arizona Federal CSI & ATSI/TSI models
(https://www.azed.gov/accountability-research/state-accountability). In order to utilize the method of the Proficiency class, one will need to provide a version of 
the Accountability Static File ().
"""

import pandas as pd
import numpy as np
from COMPONENTS import COMPONENTS
from DATABASE import DATABASE
from TABLES import TABLES
import itertools

"""
This class contains methods for creating Pandas DataFrames that contain the CSI and ATSI/TSI information displayed on ADEConnect. To find this information, navigate to 
    adeconnect.azed.gov/ -> View Applications -> Accountability -> Accountability: State and Federal Profile.
"""
class Proficiency(COMPONENTS):
    """
    Constructor for Proficiency. This class inherits from COMPONENTS, which contains variables and functionality that is shared across the different Federal CSI and ATSI/TSI Components.
    """
    def __init__(self,  fiscal_year:int=None, **kwargs):
        """
        Parameters:
        ----------
        fiscal_year (int): The current AZ D.O.E. fiscal year
        """
        super().__init__(fiscal_year=fiscal_year, **kwargs)
        self.necessary_columns = ["SAISID", "SchoolCode", "StudentGrade", "FAY", "Subject", "Performance", "TestType", "ELAMathWindow", "RAEL", 
            "SchoolTypeF", "Cohort", "Ethnicity", "SPED", "EL", "ELFEP", "IncomeEligibility1and2"]
        self.subjects_used = ["Math", "ELA"]
        self.school_type_weights_map = {key:self.federal_model_weights[key]["Proficiency"] for key in self.federal_model_weights.keys()}
        self.percent_tested_expected = .95
        self.grade_changes = {101:3}
        self.historical_grades = [3,4,5,6,7,8,9,10,11,12]
        # self.subgroup_map = {"Ethnicity":{"A":"Asian","B":"AfricanAmerican","H":"HispanicLatino","I":"NativeAmerican","P":"PacificIslander","R":"TwoorMoreRaces","W":"White"},
        #     'SPED':{1:"SWD"}, 'ELFEP':{1:"ELFEP14"}, 'IncomeEligibility1and2':{1:"IE12"}}
    

    """
    This method takes a version of the Static File, filters it for the columns and rows necessary to calculate the results of the Proficiency component, 
        and returns up to three DataFrames that contains all of the Proficiency results (CSI drilldown, CSI summary, ATSI/TSI) for each school included in the Static File provided
    """
    def calculate_component(self, static_file:pd.DataFrame, calculate_csi:bool = True, calculate_atsi:bool = True):
        """
        Parameters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (FY 2023 or beyond). It must contain the following columns: SchoolCode, SAISID, StudentGrade, FAY, Subject, 
            Performance, TestType, ELAMathWindow, RAEL, SchoolTypeF, Cohort, Ethnicity, SPED, EL, ELFEP, IncomeEligibility1and2. 

            
        Returns:
        ----------
        pd.DataFrame (if csi passed as True): drilldown csi

        pd.DataFrame (if csi passed as True): summary csi

        pd.DataFrame (if atsi passed as True): atsi/tsi table
        """
        # check that static_file contains necessary columns
        if not set(self.necessary_columns).issubset(static_file.columns): 
            missing_columns = [x for x in self.necessary_columns if x not in static_file.columns]
            raise ValueError(f"The DataFrame argument \"static_file\" is missing the following columns: {missing_columns}.")

        # rename column names for convenience and consistency
        filtered_static_file = static_file[self.necessary_columns].rename(columns={"StudentGrade":"Grade"})

        # change grade 11 to include only and all cohort 2023 students, also change federal school type to string
        filtered_static_file.replace({"TestType":{680:"", 685:"Alt"}}, inplace=True)

        # filter for grades used in proficiency calculations
        masks = []
        for model, grades in self.proficiency_grade_map.items():
            masks.append((filtered_static_file["SchoolTypeF"]==model) & (filtered_static_file["Grade"].isin(grades)).values)
        masks = np.stack(masks).any(0)
        filtered_static_file = filtered_static_file[masks]

        # convert 101 grades (3rd graders who were second grade last year) to 3
        filtered_static_file.loc[:, "Grade"] = filtered_static_file["Grade"].replace(self.grade_changes)

        # change grade column to string for grouping and column naming
        filtered_static_file["Grade"] = filtered_static_file["Grade"].astype(int).astype(str)

        # if calculating csi points, need to calculate percent tested information before filtering for FAY
        if calculate_csi: csi_percent_tested_info = self._csi_percent_tested(filtered_static_file)

        # if calculating atsi/tsi points, need to change subgroup column values and calculate percent tested information before filtering for FAY
        if calculate_atsi: 
            filtered_static_file = filtered_static_file.replace(to_replace=self.atsi_subgroups_name_changes)
            atsi_percent_tested_info = self._atsi_percent_tested(filtered_static_file)

        # TO ADD:
        # filter for assessment status

        # filter for fay, tested in math/ela, remove all assessments for RAEL 1 and 2, remove K2 model records where FAY < 2 
        static_fay_tested = filtered_static_file[(filtered_static_file["FAY"]>0) & (filtered_static_file["Subject"].isin(["ELA", "Math"])) & 
            ~(filtered_static_file["Performance"].isna()) & ~(filtered_static_file["RAEL"].isin([1,2])) & 
            ~((filtered_static_file["SchoolTypeF"]==1) & (filtered_static_file["FAY"]<2))]

        # produce csi results
        if calculate_csi:
            # get proficiency-level frequency distribution of Performance levels for each school-schooltype-grade-subject-testtype group
            grouped_counts = static_fay_tested.groupby(by=["SchoolCode", "SchoolTypeF", "Subject", "TestType", "Grade"])["Performance"].value_counts().unstack().reset_index().rename(
            columns={key:f"Number_{int(key)}" for key in static_fay_tested["Performance"].unique()}).fillna(0)

            # a reference to the new columns created above, containing counts of performance levels
            performance_columns = list(grouped_counts.columns[~grouped_counts.columns.isin(static_fay_tested.columns)])

            # sum the counts to produce number of fay students tested at each school-grade level
            grouped_counts["Number_FAY_Tested"] = grouped_counts[performance_columns].sum(axis=1)
            performance_columns = performance_columns + ["Number_FAY_Tested"]

            # iteratively group by all combinations of grade, subject, and testtype to obtain proficiency-level frequency distributions
            csi_data = grouped_counts.copy()
            grouping_columns = ["Grade", "Subject", "TestType"]
            for grouping in itertools.chain.from_iterable(itertools.combinations(grouping_columns,n) for n in range(len(grouping_columns)+1)):
                if list(grouping) == grouping_columns: continue # don't group by all of the columns since we already have that in csi_data
                temp_df = grouped_counts.groupby(by=["SchoolCode", "SchoolTypeF"] + list(grouping))[performance_columns].sum().reset_index()
                csi_data = pd.concat([csi_data, temp_df], axis=0)
            csi_data.loc[csi_data["Subject"].isna(), "Subject"] = "ELAMath" # when all subjects are combined, set subject to ELAMath
            csi_data.loc[csi_data["TestType"].isna(), "TestType"] = "All" # when all assessment types are combined, set testype to All
            csi_data.loc[csi_data["Grade"].isna(), "Grade"] = "All" # when all grades are combined, set grade to All

            # establish connection to SQL database that will be used to retrieve historical CSI data
            sql_connection = DATABASE(fiscal_year=self.fiscal_year-1, database="REDATA_UAT", schema="ssi", run="")

            # call method to create summary table:
            csi_summary = self._csi_summary(csi_data, csi_percent_tested_info, sql_connection)

            # calculate percent proficient
            csi_data["Percent_Proficient"] = (csi_data["Number_3"] + csi_data["Number_4"]) / (csi_data["Number_FAY_Tested"]) * 100
            csi_data.loc[csi_data["Number_FAY_Tested"]==0, "Percent_Proficient"] = np.nan # cannot calculate percent proficient where there are no fay_tested students

            # call method to create drilldown table:
            csi_drilldown = self._csi_drilldown(csi_data, sql_connection)

        else: csi_summary, csi_drilldown = None, None
        if calculate_atsi:
            groupings = self._get_all_pairs(["Subject"], self.subgroups) # returns a list of lists, in which each list is a set of columns we want to group by

            atsi_data = [] # holds the groupby results for each group
            # for each Subject-Subgroup combination, apply a groupby function to get frequency distribution of Proficiency levels
            for grouping in groupings:
                grouped_data = static_fay_tested.groupby(by=["SchoolCode","SchoolTypeF"] + grouping)["Performance"].value_counts().unstack().reset_index().rename(
                    columns={key:f"Number_{int(key)}" for key in static_fay_tested["Performance"].unique()}).fillna(0)
                grouped_data.rename(columns={x:"Subgroup" for x in self.subgroups}, inplace=True)
                atsi_data.append(grouped_data)
            atsi_data:pd.DataFrame = pd.concat(atsi_data)
            atsi_data = atsi_data[(atsi_data["Subgroup"]!=0) & (atsi_data["Subgroup"]!="U")] # drop all of the non-subgroup groups
            atsi_data.loc[atsi_data["Subject"].isna(), "Subject"] = "ELAMath" # when grouping by Subgroup and not Subject, set Subject to ELAMath (All)
            atsi_data.loc[atsi_data["Subgroup"].isna(), "Subgroup"] = "All" # when grouping by Subject and not by Subgroup, set the Subgroup to All


            performance_columns = atsi_data.columns[~atsi_data.columns.isin(static_fay_tested.columns)] # a reference to the new columns created above, containing counts of performance levels
            atsi_data["Number_FAY_Tested"] = atsi_data[list(performance_columns)].sum(axis=1) # sum proficiency-level counts to get total tested

            atsi_summary = self._atsi_summary(atsi_data, atsi_percent_tested_info) # get summary ATSI data (appears at top of GUI)
            atsi_drilldown = self._atsi_drilldown(atsi_data, atsi_percent_tested_info) # get drilldown ATSI data (appears in the Proficiency section of GUI)
            # historical_atsi = self._atsi_historical(atsi_data) # get historical ATSI data (also appears in the Proficiency section of GUI)

            # combine all of the data
            atsi = pd.merge(atsi_summary, atsi_drilldown, on=["SchoolCode"], how="outer").rename(columns={"SchoolCode":"EntityID"}) #.merge(historical_atsi, on=["SchoolCode"], how="left")
            atsi.sort_values(by=["SchoolTypeF"], inplace=True)
        else: atsi = None
                
        return csi_summary, csi_drilldown, atsi


    
    """
    Calculates the percent tested and 95% tested student penalty for each school and school-grade pairing. This is a protected method and should only be called 
        from within the Proficiency class.
    """
    def _csi_percent_tested(self, filtered_static_file:pd.DataFrame):
        """
        Parameters:
        ----------
        filtered_static_file (pd.DataFrame): A version of the static file that has been filtered for important columns and the grades that are included in the 
            Proficiency calculations for each federal school model

        Returns:
        ----------
        pd.DataFrame (a pandas DataFrame containing percent tested measures and 95% tested student penalties for every school and school-grade grouping)
        """
        # pull out students that were enrolled during the testing window
        # static_enrolled = filtered_static_file.query("ELAMathWindow == 1") 
        static_enrolled = filtered_static_file[filtered_static_file["ELAMathWindow"]==1]

        # add a copy of the table provided to itself with Grade set to All
        static_enrolled["Grade"] = "All"
        static_enrolled = pd.concat([filtered_static_file, static_enrolled], axis=0)

        # group by school-grade and calculate number of students enrolled in each group during the math/ela testing window
        csi_number_enrolled = static_enrolled.groupby(by=["SchoolCode", "SchoolTypeF", "Grade"])["SAISID"].agg("nunique").rename("Number_Enrolled")*2
        
        # group by school-grade and calculate number of students assessed in each group in math/ela
        csi_number_tested = static_enrolled[(static_enrolled["Performance"].notna()) & (static_enrolled["Subject"].isin(['ELA', 'Math']))].groupby(
            by=["SchoolCode", "SchoolTypeF", "Grade"])["SAISID"].agg("count").rename("Number_Tested")

        # combine the information calculated above
        csi_percent_tested_info = pd.merge(csi_number_enrolled, csi_number_tested, left_index=True, right_index=True, how="left").reset_index().fillna(0)

        # calculate percent tested and 95% tested student penalty for each school
        csi_percent_tested_info["Percent_Tested"] = csi_percent_tested_info["Number_Tested"] / csi_percent_tested_info["Number_Enrolled"] * 100 # calculate the percent tested for each school-grade model
        # csi_percent_tested_info["Student_Penalty"] = np.maximum(0, np.floor(self.percent_tested_expected*csi_percent_tested_info["Number_Enrolled"]) - csi_percent_tested_info["Number_Tested"])  # calculate the percent multiplier for each school-grade model
        csi_percent_tested_info["Percent_Multiplier"] = csi_percent_tested_info["Number_Tested"] / np.floor(self.percent_tested_expected*csi_percent_tested_info["Number_Enrolled"]) # calculate the percent multiplier for each school-grade model
        # csi_percent_tested_info.loc[csi_percent_tested_info["SchoolTypeF"]==1, ["Percent_Tested", "Student_Penalty"]] = [np.nan, 0] # K2 model ignored 95% tested penalty
        csi_percent_tested_info.loc[csi_percent_tested_info["SchoolTypeF"]==1, ["Percent_Tested", "Percent_Multiplier"]] = [np.nan, 1] # K2 model ignored 95% tested penalty
        # csi_percent_tested_info = csi_percent_tested_info[["SchoolCode", "SchoolTypeF", "Grade", "Percent_Tested", "Student_Penalty"]]
        csi_percent_tested_info = csi_percent_tested_info[["SchoolCode", "SchoolTypeF", "Grade", "Percent_Tested", "Percent_Multiplier"]]
        return csi_percent_tested_info
    

    """
    Calculates the percent tested and 95% tested student penalty for each school-subject-subgroup grouping. This is a protected method and should only be called 
        from within the Proficiency class.
    """
    def _atsi_percent_tested(self, filtered_static_file:pd.DataFrame):
        """
        Parameters:
        ----------
        filtered_static_file (pd.DataFrame): A version of the static file that has been filtered for important columns and the grades that are included in the 
            Proficiency calculations for each federal school model

        Returns:
        ----------
        pd.DataFrame (a pandas DataFrame containing percent tested measures and 95% tested student penalties for every school-subject-subgroup grouping)
        """
        # pull out students that were enrolled during the testing window
        static_enrolled = filtered_static_file[filtered_static_file["ELAMathWindow"]==1]
        groupings = [[x] for x in self.subgroups]
        groupings.append([])
        atsi_number_enrolled = [] # used to hold groupby results containing number of students enrolled
        atsi_number_tested = [] # used to hold groupby results containing number of students assessed
        for grouping in groupings:
            # group by school-subgroup and calculate number of students enrolled
            number_enrolled = (static_enrolled.groupby(by=["SchoolCode", "SchoolTypeF",] + grouping)["SAISID"].agg("nunique").rename("Number_Enrolled")*2).reset_index()
            number_enrolled.rename(columns={"Ethnicity":"Subgroup", "SPED":"Subgroup", "ELFEP":"Subgroup", "IncomeEligibility1and2":"Subgroup"}, inplace=True)
            atsi_number_enrolled.append(number_enrolled)
            
            # group by school-subject-subgroup and calculate number of students assessed
            number_tested = static_enrolled[(static_enrolled["Performance"].notna()) & (static_enrolled["Subject"].isin(['ELA', 'Math']))].groupby(
                by=["SchoolCode", "SchoolTypeF", "Subject"] + grouping)["SAISID"].agg("count").rename("Number_Tested").reset_index()
            number_tested.rename(columns={"Ethnicity":"Subgroup", "SPED":"Subgroup", "ELFEP":"Subgroup", "IncomeEligibility1and2":"Subgroup"}, inplace=True)
            atsi_number_tested.append(number_tested)

            # group by school-subgroup and calculate number of students assessed
            number_tested = static_enrolled[(static_enrolled["Performance"].notna()) & (static_enrolled["Subject"].isin(['ELA', 'Math']))].groupby(
                by=["SchoolCode", "SchoolTypeF"] + grouping)["SAISID"].agg("count").rename("Number_Tested").reset_index()
            number_tested.rename(columns={"Ethnicity":"Subgroup", "SPED":"Subgroup", "ELFEP":"Subgroup", "IncomeEligibility1and2":"Subgroup"}, inplace=True)
            atsi_number_tested.append(number_tested)

        # concatenate the enrollment counts together
        atsi_number_enrolled:pd.DataFrame = pd.concat(atsi_number_enrolled) 
        atsi_number_enrolled.loc[atsi_number_enrolled["Subgroup"].isna(), "Subgroup"] = "All" # fill in missing values created when grouping by school and subject only (without subgroup)

        # duplicate the enrollment counts for each subject. Every subject should have the same enrollment count since all students are expected to 
        # be assessed on each subject. This is necessary because later we merge the enrollment counts and tested counts on subject in order to keep all three at any school-subgroup where the count of tested is zero.
        dfs = []
        for subject in ["Math", "ELA", "ELAMath"]:
            temp_df = atsi_number_enrolled.copy()
            temp_df["Subject"] = subject
            dfs.append(temp_df)
        atsi_number_enrolled = pd.concat(dfs, axis=0)

        # concatenate the number tested counts together
        atsi_number_tested:pd.DataFrame = pd.concat(atsi_number_tested)
        atsi_number_tested.loc[atsi_number_tested["Subgroup"].isna(), "Subgroup"] = "All" # fill in missing values created when grouping by school and subject only (without subgroup)
        atsi_number_tested.loc[atsi_number_tested["Subject"].isna(), "Subject"] = "ELAMath" # fill in missing values created when grouping by school and subgroup only (without subject)

        # combine enrollment and number tested data
        # TODO: account for number enrolled values of zero introduced by code above
        atsi_percent_tested_info = pd.merge(atsi_number_enrolled, atsi_number_tested, on=["SchoolCode", "Subgroup", "Subject", "SchoolTypeF"], how="left").reset_index().fillna(0)
        atsi_percent_tested_info = atsi_percent_tested_info[(atsi_percent_tested_info["Subgroup"]!=0) & (atsi_percent_tested_info["Subgroup"]!="U")] # drop the subgroups we don't use

        # calculate percent tested and 95% tested student penalty for each school-subject-subgroup
        atsi_percent_tested_info["Percent_Tested"] = atsi_percent_tested_info["Number_Tested"] / atsi_percent_tested_info["Number_Enrolled"] * 100 # calculate the percent tested for each group
        # atsi_percent_tested_info["Student_Penalty"] = np.maximum(0, np.floor(self.percent_tested_expected*atsi_percent_tested_info["Number_Enrolled"]) - atsi_percent_tested_info["Number_Tested"]) # calculate student penalty for each group
        atsi_percent_tested_info["Percent_Multiplier"] = atsi_percent_tested_info["Number_Tested"] / np.floor(self.percent_tested_expected*atsi_percent_tested_info["Number_Enrolled"]) # calculate the percent multiplier for each school-grade model
        # atsi_percent_tested_info = atsi_percent_tested_info[["SchoolCode", "SchoolTypeF", "Subject", "Subgroup", "Percent_Tested", "Student_Penalty"]]
        atsi_percent_tested_info = atsi_percent_tested_info[["SchoolCode", "SchoolTypeF", "Subject", "Subgroup", "Percent_Tested", "Percent_Multiplier"]]
        return atsi_percent_tested_info


    """
    Calculates the percent proficient for each school-grade pairing, applying 95% tested student penalties. Also calculates the final Proficiency score for each school 
        and combines these results with historical Proficiency data. This is a protected method and should only be called from within the Proficiency class.
    """
    def _csi_summary(self, csi_data:pd.DataFrame, csi_percent_tested_info:pd.DataFrame, sql_connection:DATABASE):
        """
        Parameters:
        ----------
        csi_data (pd.DataFrame): A table containing counts of each proficiency level for every school-subject-grade group

        csi_percent_tested_info (pd.DataFrame): A table containing percent tested and 95% tested student penalty measures for every school-subject-grade group

        sql_connection (DATABASE): A connection to the SQL database that is used for obtaining historical CSI data

        Returns:
        ----------
        pd.DataFrame: A table containing all of the Proficiency-related information (percent proficient, proficiency points, historical results, etc.) that is 
            included in the CSI summary GUI table
        """
        csi_summary = csi_data[(csi_data["Subject"]=="ELAMath") & (csi_data["TestType"]=="All")] # filter for all subjects and all testtypes
        csi_summary["Number_Proficient"] = csi_summary["Number_3"] + csi_summary["Number_4"] # compute number of students that scored Proficient
        csi_summary = pd.merge(csi_summary, csi_percent_tested_info, on=["SchoolCode", "SchoolTypeF", "Grade"], how="outer") # merge proficiency level info with percent tested info

        # calculate percent proficient
        # csi_summary["Percent_Proficient"] = (csi_summary["Number_Proficient"]) / (csi_summary["Number_FAY_Tested"]+csi_summary["Student_Penalty"]) * 100
        csi_summary["Percent_Proficient"] = csi_summary["Number_Proficient"] / csi_summary["Number_FAY_Tested"] * 100 * csi_summary["Percent_Multiplier"]
        
        # convert Grade column to wide format (resulting in a column for each grade and value defined below)
        csi_summary["Grade"] = "Grade" + csi_summary["Grade"]
        csi_summary_wide = csi_summary.pivot(index=["SchoolCode", "SchoolTypeF"], columns=["Grade"], values=["Number_Proficient", "Percent_Proficient", "Number_FAY_Tested"]).reset_index()

        # rename columns to match previous year's results
        columns = csi_summary_wide.columns.to_frame()
        columns = columns.replace({"Number_FAY_Tested":"FayStud", "Number_Proficient":"NumProficient", "Percent_Proficient":"PctProficient"}, regex=True)
        columns = columns.iloc[:,0] + columns.iloc[:,1]
        columns = columns.replace({"FayStudGradeAll":"FayStudAllStudents", "NumProficientGradeAll":"NumProfAllStudents", "PctProficientGradeAll":"PctProfAllStudents"})
        csi_summary_wide.columns = columns

        # select for all the proficiency results of all grades (entire school)
        all_grades_summary = csi_summary[csi_summary["Grade"]=="GradeAll"]

        # calculate Proficiency score for each school
        all_grades_summary["Proficiency"] = all_grades_summary["Percent_Proficient"] / 100 * csi_summary["SchoolTypeF"].replace(self.school_type_weights_map)

        # change points if school does not meet the N-count
        all_grades_summary.loc[all_grades_summary["Number_FAY_Tested"]<(self.n_count*2), "Proficiency"] = np.nan # "NR"
        all_grades_summary[f"ProficiencyPoints"] = all_grades_summary["Proficiency"] # a copy of the score for the Proficiency gui cells
        all_grades_summary[f"ProficiencyPoints{self.fiscal_year}"] = all_grades_summary["Proficiency"] # a copy of the score for the historical proficiency gui cells

        # obtain historical csi summary data
        # csi_historical_summary = self._csi_historical_summary(csi_data, sql_connection=sql_connection)

        # merge the proficiency results for each grade, school, and historical together
        csi_summary_wide = pd.merge(all_grades_summary[["SchoolCode", "Proficiency", "ProficiencyPoints", f"ProficiencyPoints{self.fiscal_year}", "Percent_Tested"]], 
            csi_summary_wide, on=["SchoolCode"], how="outer")
        # csi_summary_wide = pd.merge(csi_summary_wide, csi_historical_summary, on=["SchoolCode"], how="left")
        csi_summary_wide = csi_summary_wide.rename(columns={"SchoolCode":"EntityID", "Percent_Tested":"PercentTested"}) # rename to match previous year's results
        csi_summary_wide.sort_values(by=["SchoolTypeF"], inplace=True)
        return csi_summary_wide


    """
    Calculates all of the information (percent proficient, Number FAY Tested, etc.) that is present in the Federal CSI Proficiency drilldown GUI tables on ADEConnect. 
        This is a protected method that should only be called from within the Proficiency class.
    """
    def _csi_drilldown(self, csi_data:pd.DataFrame, sql_connection:DATABASE):
        """
        Parameters:
        ----------
        csi_data (pd.DataFrame): A table containing counts of each proficiency level for every school-subject-grade group

        sql_connection (DATABASE): A connection to the SQL database that is used for obtaining historical CSI data

        Returns:
        ----------
        pd.DataFrame: A table containing all of the information (Percent Proficient, Number FAY Tested, etc.) that is present in the CSI drilldown GUI table on ADEConnect
        """
        # pivot to get drilldown columns (one column for every column and value below)
        csi_drilldown = csi_data.pivot(index=["SchoolCode", "SchoolTypeF"], columns=["Grade", "Subject", "TestType"], 
            values=["Number_1", "Number_2", "Number_3", "Number_4", "Number_FAY_Tested", "Percent_Proficient"]).reset_index()

        # rename columns to match previous year's naming convention
        columns = csi_drilldown.columns.to_frame()
        columns = columns.iloc[:,3] + columns.iloc[:,2] + columns.iloc[:,1] + columns.iloc[:,0]
        columns = columns.replace({"Number":"Num", "Percent":"Pct", "Proficient":"Prof", "_":""}, regex=True).replace({"Num1":"NumMP", "Num2":"NumPP", "Num3":"NumP", "Num4":"NumHP"}, regex=True)
        csi_drilldown.columns = columns

        # obtain historical csi drilldown information (percent proficiencies) - also contains current fiscal_year results
        csi_historical_drilldown = self._csi_historical_drilldown(csi_data, sql_connection=sql_connection)

        # combine drilldown and historical information
        csi_drilldown = pd.merge(csi_drilldown, csi_historical_drilldown, on=["SchoolCode"], how="left")
        csi_drilldown.sort_values(by=["SchoolTypeF"], inplace=True)
        csi_drilldown["FiscalYear"] = self.fiscal_year
        csi_drilldown.replace({"SchoolTypeF":self.federal_school_type_map}, inplace=True)
        csi_drilldown = csi_drilldown.rename(columns={"SchoolCode":"EntityID", "SchoolTypeF":"FederalModel"})
        
        # get all grades included in the results
        # grades_included = set(itertools.chain(*self.proficiency_grade_map.values())) 
        # for key, value in self.grade_changes.items():
        #     grades_included.remove(key) # replace 101 with 3 in order to have K2 model results show grade 3 instead of 101
        #     grades_included.add(value)

        # HERE need to set current fiscal_year column

        # define columns and order that need to be in the drilldown table
        # ordered_columns = TABLES(fiscal_year=self.fiscal_year).get_csi_proficiency_drilldown_columns(grades=list(grades_included))

        # missing_columns = [x for x in ordered_columns if x not in csi_drilldown.columns]
        # csi_drilldown.loc[:, missing_columns] = np.nan

        # missing_columns = set(ordered_columns).difference(csi_drilldown.columns)
        # missing_historical_columns = [x for x in missing_columns if str(self.fiscal_year) in x]
        # csi_drilldown[missing_historical_columns] = np.nan # replace historical columns that are not being measured this year with NAN

        # # HERE want to print which columns are missing
        # # check that all of the historical columns expected were produced
        # if set(missing_columns).difference(missing_historical_columns):
        #     csi_drilldown.loc[: missing_columns] = np.nan
        #     # raise KeyError(f"The CSI drilldown table is missing the following columns: {set(missing_columns).difference(missing_historical_columns)}.")
        # csi_drilldown = csi_drilldown[ordered_columns]
        return csi_drilldown
    

    """
    Calculates percent proficient and proficiency points earned by each school-subgroup pair, applying the 95% tested penalty. This is a protected method that should only 
        be called from within the Proficiency class.
    """
    def _atsi_summary(self, atsi_data:pd.DataFrame, atsi_percent_tested_info:pd.DataFrame):
        """
        Parameters:
        ----------
        atsi_data (pd.DataFrame): A table containing counts of each proficiency level for every school-subject-subgroup group

        atsi_percent_tested_info (pd.DataFrame): A table containing percent tested and 95% tested student penalty measures for every school-subject-subgroup group

        Returns:
        ----------
        pd.DataFrame: A table containing the Proficiency-related information (Proficiency Points, Percent Tested, etc.) that is present at the top of the ATSI/TSI GUI table on ADEConnect
        """
        # combine proficiency levels and percent tested info
        atsi_summary = pd.merge(atsi_data[atsi_data["Subject"]=="ELAMath"], atsi_percent_tested_info[atsi_percent_tested_info["Subject"]=="ELAMath"], # filter for all subjects
            on=["SchoolCode", "SchoolTypeF", "Subject", "Subgroup"], how="outer")
        # atsi_summary.loc[atsi_summary["SchoolTypeF"]==1, "Student_Penalty"] = 0 # K2 model ignored 95% tested penalty

        # calculate percent proficient, applying 95% tested student penalty
        # atsi_summary["Percent_Proficient"] = (atsi_summary["Number_3"] + atsi_summary["Number_4"]) / (atsi_summary["Number_FAY_Tested"]+atsi_summary["Student_Penalty"]) * 100
        atsi_summary["Percent_Proficient"] = (atsi_summary["Number_3"] + atsi_summary["Number_4"]) / atsi_summary["Number_FAY_Tested"] * 100 * atsi_summary["Percent_Multiplier"]
        atsi_summary.loc[atsi_summary["Number_FAY_Tested"]==0, "Percent_Proficient"] = np.nan

        atsi_summary["Model_Weight"] = atsi_summary["SchoolTypeF"].replace(self.school_type_weights_map)

        # assign points
        atsi_summary["Proficiency_Points"] = atsi_summary["Percent_Proficient"]/100*atsi_summary["Model_Weight"]
        atsi_summary.loc[atsi_summary["Number_FAY_Tested"]<(self.n_count*2), "Proficiency_Points"] = np.nan #"NR" # change point value if subgroup does not meet the N-count

        # convert to wide format (one column for each column and value defined below)
        atsi_summary = atsi_summary.pivot(index=["SchoolCode", "SchoolTypeF"], columns=["Subgroup"], values=["Percent_Proficient", "Percent_Tested", "Proficiency_Points"]).reset_index()
        # atsi_summary = atsi_summary.re

        # change column names
        columns = atsi_summary.columns.to_frame()
        columns = columns.iloc[:,0] + columns.iloc[:,1]
        columns = columns.replace({"Percent_Tested":"PercentTested", "Proficiency_Points":"Proficiency"}, regex=True)
        columns.loc[columns.str.contains("Percent_Proficient")] = f"FY{self.fiscal_year}Pct" + columns.loc[columns.str.contains("Percent_Proficient")].replace({"Percent_Proficient":""}, regex=True)
        atsi_summary.columns = columns
        return atsi_summary
    

    """
    Calculates the percent proficient for each school-subject-subgroup grouping and applies the 95% tested penalty to each.
      This is a protected method that should only be called from within the Proficiency class.
    """
    def _atsi_drilldown(self, atsi_data:pd.DataFrame, atsi_percent_tested_info:pd.DataFrame):
        """
        Parameters:
        ----------
        atsi_data (pd.DataFrame): A table containing counts of each proficiency level for every school-subject-subgroup group

        Returns:
        ----------
        pd.DataFrame: A table containing Proficiency-related information (Percent Proficient, Number FAY Tested, etc.) for each school-subject-subgroup grouping that is present 
            in the ATSI/TSI drilldown GUI table on ADEConnect
        """
        atsi_drilldown = pd.merge(atsi_data, atsi_percent_tested_info, on=["SchoolCode", "Subject", "Subgroup", "SchoolTypeF"], how="outer")
        # atsi_drilldown.loc[atsi_drilldown["SchoolTypeF"]==1, "Student_Penalty"] = 0 # K2 model ignored 95% tested penalty
        atsi_drilldown.loc[atsi_drilldown["SchoolTypeF"]==1, "Percent_Multiplier"] = 1 # K2 model ignored 95% tested penalty

        # atsi_drilldown["Percent_Proficient"] = (atsi_drilldown["Number_3"] + atsi_drilldown["Number_4"]) / (
        #     atsi_drilldown["Number_FAY_Tested"]+atsi_drilldown["Student_Penalty"]) * 100 # calculate percent proficient
        atsi_drilldown["Percent_Proficient"] = (atsi_drilldown["Number_3"] + atsi_drilldown["Number_4"]) / atsi_drilldown["Number_FAY_Tested"] * 100 * atsi_drilldown["Percent_Multiplier"]

        # convert to wide format (one column for each column and value defined below)
        atsi_drilldown = atsi_drilldown.pivot(index=["SchoolCode"], columns=["Subgroup", "Subject"], values=["Number_1", "Number_2", "Number_3", "Number_4",
            "Number_FAY_Tested", "Percent_Proficient"]).reset_index()
        
        # change column names
        columns = atsi_drilldown.columns.to_frame()
        columns = columns.iloc[:,2] + columns.iloc[:,0] + columns.iloc[:,1]
        columns = columns.replace({"Number":"Num", "Tested":"Tstd", "Percent":"Pct", "Proficient":"Prof", "_":""}, regex=True).replace({"Num1":"NumMP", "Num2":"NumPP", "Num3":"NumP", "Num4":"NumHP"}, regex=True)
        atsi_drilldown.columns = columns
        return atsi_drilldown
    

    """
    Obtains percent proficiency measures for every school-subject-grade grouping from the past three years using previous tables from the SQL server connection provided. 
        This is a protected method and should only be called from within the Proficiency class.
    """
    def _csi_historical_drilldown(self, csi_data:pd.DataFrame, sql_connection:DATABASE):
        """
        Parameters:
        ----------
        csi_data (pd.DataFrame): A table containing counts of each proficiency level for every school-subject-grade group

        sql_connection (DATABASE): A connection to the SQL database that is used for obtaining historical CSI data

        Returns:
        ----------
        pd.DataFrame: A table containing all of the historical information (Percent Proficient values for every school-subject-grade grouping from the past three years) that is 
            present in the CSI drilldown GUI table on ADEConnect
        """
        historical_years = [self.fiscal_year-3, self.fiscal_year-2, self.fiscal_year-1] # define the past three years using the current fiscal year

        # define all of the historical columns that need to be returned
        subjects = csi_data["Subject"].unique()
        test_types = ["All", "Alt", ""] # using hard-coded values here because some of these values were missing from initial static file version
        columns = ["EntityID"] # holds the columns
        for subject in subjects:
            for grade in self.historical_grades: # including all grades that have ever been included in Proficiency calculations
                for test_type in test_types:
                    for year in historical_years:
                        columns.append(str(test_type) + str(subject) + str(grade) + str(year))

        # join each federal model's sql table together
        csi_historical_drilldown_tables = []
        for model, table in self.py_prof_dd_tables_by_model.items():
            temp_df = sql_connection.read_table(table_name = table)
            temp_df = temp_df[temp_df["EntityID"].isin(csi_data[csi_data["SchoolTypeF"]==model])] # removes schools that shouldn't be in last year's table
            temp_df = temp_df[list(set(columns) & set(temp_df.columns))]
            csi_historical_drilldown_tables.append(temp_df)
        csi_historical_drilldown = pd.concat(csi_historical_drilldown_tables, axis=0).rename(columns={"EntityID":"SchoolCode"})

        # calculate percent proficiency values for the current fiscal year to be combined with the historical results above
        current_year_data = csi_data[csi_data["Grade"]!="All"]
        current_year_data = current_year_data.pivot(index=["SchoolCode"], columns=["Grade", "Subject", "TestType"], values=["Percent_Proficient"]).reset_index()
        columns = current_year_data.columns.to_frame()
        columns = columns.iloc[:,3] + columns.iloc[:,2] + columns.iloc[:,1] + columns.iloc[:,0]
        current_year_data.columns = columns.replace({"Number":"Num", "Percent":"Pct", "Proficient":"Prof", "_":""}, regex=True)
        current_year_data.columns = current_year_data.columns.str.replace("PctProf", str(self.fiscal_year), regex=True)

        # combine historical and current fiscal year percent proficiencies
        csi_historical_drilldown = pd.merge(csi_historical_drilldown, current_year_data, on=["SchoolCode"], how="right")
        return csi_historical_drilldown
    


    """
    Obtains Proficiency Points for each school from the past three years using the previous CSI summary table from the SQL server connection provided. 
        This is a protected method and should only be called from within the Proficiency class.
    """
    def _csi_historical_summary(self, csi_data:pd.DataFrame, sql_connection:DATABASE):
        """
        Parameters:
        ----------
        csi_data (pd.DataFrame): A table containing counts of each proficiency level for every school-subject-grade group

        sql_connection (DATABASE): A connection to the SQL database that is used for obtaining historical CSI data

        Returns:
        ----------
        pd.DataFrame: A table containing the historical Proficiency-related information (Points) that is present in the CSI summary GUI table on ADEConnect
        """

        # load in historical csi proficiency points data
        historical_years = [self.fiscal_year-3, self.fiscal_year-2, self.fiscal_year-1]

        # define the columns that must be obtained from historical table
        columns = ["EntityID"]
        for year in historical_years:
            columns.append("ProficiencyPoints" + str(year))

        # join each Federal Model's gui table together
        csi_historical_summary_tables = []
        for model, table in self.py_summary_tables_by_model.items():
            temp_df = sql_connection.read_table(table_name = table)
            temp_df = temp_df[temp_df["EntityID"].isin(csi_data[csi_data["SchoolTypeF"]==model])] # removes schools that shouldn't be in last year's table
            temp_df = temp_df[list(set(columns) & set(temp_df.columns))]
            csi_historical_summary_tables.append(temp_df)
        csi_historical_summary = pd.concat(csi_historical_summary_tables, axis=0).rename(columns={"EntityID":"SchoolCode"})

        return csi_historical_summary

    
    """
    Obtains Percent Proficient values for all school-subgroup pairs from the past four years. This is a protected method that should only be called from within the Proficiency class
    """
    def _atsi_historical(self, atsi_data:pd.DataFrame):
        """
        Parameters:
        ----------
        atsi_data (pd.DataFrame): A table containing counts of each proficiency level for every school-subject-subgroup group

        Returns:
        ----------
        pd.DataFrame: A table containing the Proficiency-related historical information (Percent Proficient) that is present in the ATSI/TSI GUI table on ADEConnect
        """
        # define previous four historical years
        historical_years = [self.fiscal_year-4, self.fiscal_year-3, self.fiscal_year-2, self.fiscal_year-1]

        # define historical columns that must be included
        subgroups = atsi_data["Subgroup"].unique()
        columns = ["SchoolCode"]
        for subgroup in subgroups:
            for year in historical_years:
                columns.append("FY" + str(year) + "Pct" + str(subgroup))
        
        # for testing with 2022 static file: reads from 2022 ATSI table (not really the previous one)
        # if self.fiscal_year == 2022:
        #     sql_connection = DATABASE(fiscal_year=self.fiscal_year, run = 'Final', schema = 'Results', database = 'AccountabilityArchive')
        # usual approach - reads from previous year's table
        # else:
        sql_connection = DATABASE(fiscal_year = self.fiscal_year-1, run = 'Final', schema = 'Results', database = 'AccountabilityArchive')
        atsi_historical_data = sql_connection.read_table(table_name=self.py_atsi_tables["atsi_1"]).rename({"EntityID":"SchoolCode"})

        # keep only the historical columns defined above, can drop the rest
        atsi_historical_data = atsi_historical_data[[x for x in columns if x in atsi_historical_data.columns]] # 

        # for any columns missing from historical table, fill with NAN
        atsi_historical_data.loc[:, [x for x in columns if x not in atsi_historical_data.columns]] = np.nan 
        return atsi_historical_data


    """
    Returns a list containing all of the combinations of the elements from l1 and l2 provided, plus their subsets. 
        Ex: l1=[a,b,c], l2=[d,e], returns [[a,d], [a,e], [b,d], [b,e], [c,d], [c,e], [a], [b], [c], [d], [e], []]
    """
    def _get_all_pairs(self, l1:list, l2:list):
        new_list = []
        new_list.append([])
        for x in l1:
            new_list.append([x])
            for y in l2:
                new_list.append([y])
                new_list.append([x,y])
        return new_list
    



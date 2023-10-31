# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 13:58:27 2023

@author: YFahmy
"""
import pandas as pd
from COMPONENTS import COMPONENTS

class TABLES(COMPONENTS):
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.fy_minus_2 = self.fiscal_year-2
        self.fy_minus_3 = self.fiscal_year-3
        self.fy_minus_4 = self.fiscal_year-4
    
    def get_csi_growth_drilldown_columns(self):
        names = '''[FederalModel],[EntityID],[FiscalYear],[ELAFAYStdGrowthScoreGrade4],[ELAMedianSGPGrade4],[ELAFAYStdGrowthScoreGrade5]
                  ,[ELAMedianSGPGrade5],[ELAFAYStdGrowthScoreGrade6],[ELAMedianSGPGrade6],[ELAFAYStdGrowthScoreGrade7]
                  ,[ELAMedianSGPGrade7],[ELAFAYStdGrowthScoreGrade8],[ELAMedianSGPGrade8],[ELAMedianSGPAllStudents]
                  ,[MathFAYStdGrowthScoreGrade4],[MathMedianSGPGrade4],[MathFAYStdGrowthScoreGrade5],[MathMedianSGPGrade5]
                  ,[MathFAYStdGrowthScoreGrade6],[MathMedianSGPGrade6],[MathFAYStdGrowthScoreGrade7],[MathMedianSGPGrade7]
                  ,[MathFAYStdGrowthScoreGrade8],[MathMedianSGPGrade8],[MathMedianSGPAllStudents],[Grade4ELA0thru19]
                  ,[Grade4ELA20thru39],[Grade4ELA40thru59],[Grade4ELA60thru79],[Grade4ELA80thru100],[Grade4Math0thru19]
                  ,[Grade4Math20thru39],[Grade4Math40thru59],[Grade4Math60thru79],[Grade4Math80thru100]
                  ,[Grade5ELA0thru19],[Grade5ELA20thru39],[Grade5ELA40thru59],[Grade5ELA60thru79],[Grade5ELA80thru100]
                  ,[Grade5Math0thru19],[Grade5Math20thru39],[Grade5Math40thru59],[Grade5Math60thru79]
                  ,[Grade5Math80thru100],[Grade6ELA0thru19],[Grade6ELA20thru39]
                  ,[Grade6ELA40thru59],[Grade6ELA60thru79],[Grade6ELA80thru100],[Grade6Math0thru19]
                  ,[Grade6Math20thru39],[Grade6Math40thru59],[Grade6Math60thru79],[Grade6Math80thru100]
                  ,[Grade7ELA0thru19],[Grade7ELA20thru39],[Grade7ELA40thru59],[Grade7ELA60thru79]
                  ,[Grade7ELA80thru100],[Grade7Math0thru19],[Grade7Math20thru39],[Grade7Math40thru59]
                  ,[Grade7Math60thru79],[Grade7Math80thru100],[Grade8ELA0thru19],[Grade8ELA20thru39]
                  ,[Grade8ELA40thru59],[Grade8ELA60thru79],[Grade8ELA80thru100],[Grade8Math0thru19]
                  ,[Grade8Math20thru39],[Grade8Math40thru59],[Grade8Math60thru79],[Grade8Math80thru100]'''
        names = names.split(',')
        names = pd.Series(names)
        names = names.str.replace('[','', regex=True).str.replace(']','', regex=True).str.strip()
        return names
    


    def get_csi_proficiency_drilldown_columns(self, grades:list = [3,4,5,6,7,8,11]):
        columns = ["FederalModel", "EntityID", "FiscalYear"]
        for subject in ["ELA", "Math", "ELAMath", "AltELA", "AltMath", "AltELAMath","AllELA","AllMath","AllELAMath"]:
            for grade in (sorted(grades) + ["All"]):
                for value in ["NumFAYTested","NumMP","NumPP","NumP","NumHP","PctProf"]:
                    columns.append(subject+str(grade)+value)
            for grade in ([3,4,5,6,7,8,9,10,11,12]):
                for year in [self.fiscal_year-3, self.fiscal_year-2, self.fiscal_year-1, self.fiscal_year]:
                    columns.append(subject+str(grade)+str(year))
        return columns

    
    def get_csi_ca_drilldown_columns(self):
        names = f'''[FederalModel],[EntityID]
          ,[FiscalYear],[NumberChronicallyAbsent1],[PercentChronicallyAbsent1],[NumberChronicallyAbsent2]
          ,[PercentChronicallyAbsent2],[NumberChronicallyAbsent3],[PercentChronicallyAbsent3]
          ,[NumberChronicallyAbsent4],[PercentChronicallyAbsent4],[NumberChronicallyAbsent5]
          ,[PercentChronicallyAbsent5],[NumberChronicallyAbsent6],[PercentChronicallyAbsent6]
          ,[NumberChronicallyAbsent7],[PercentChronicallyAbsent7],[NumberChronicallyAbsent8]
          ,[PercentChronicallyAbsent8],[NumberChronicallyAbsentAll],[PercentChronicallyAbsentAll]
          ,[PctChronicallyAbsent{self.fy_minus_3}Grade1],[PctChronicallyAbsent{self.fy_minus_2}Grade1]
          ,[PctChronicallyAbsent{self.previous_fiscal_year}Grade1]
          ,[PctChronicallyAbsent{self.fiscal_year}Grade1],[PctChronicallyAbsent{self.fy_minus_3}Grade2]
          ,[PctChronicallyAbsent{self.fy_minus_2}Grade2]
          ,[PctChronicallyAbsent{self.previous_fiscal_year}Grade2],[PctChronicallyAbsent{self.fiscal_year}Grade2]
          ,[PctChronicallyAbsent{self.fy_minus_3}Grade3]
          ,[PctChronicallyAbsent{self.fy_minus_2}Grade3],[PctChronicallyAbsent{self.previous_fiscal_year}Grade3]
          ,[PctChronicallyAbsent{self.fiscal_year}Grade3]
          ,[PctChronicallyAbsent{self.fy_minus_3}Grade4],[PctChronicallyAbsent{self.fy_minus_2}Grade4]
          ,[PctChronicallyAbsent{self.previous_fiscal_year}Grade4]
          ,[PctChronicallyAbsent{self.fiscal_year}Grade4],[PctChronicallyAbsent{self.fy_minus_3}Grade5]
          ,[PctChronicallyAbsent{self.fy_minus_2}Grade5]
          ,[PctChronicallyAbsent{self.previous_fiscal_year}Grade5],[PctChronicallyAbsent{self.fiscal_year}Grade5]
          ,[PctChronicallyAbsent{self.fy_minus_3}Grade6]
          ,[PctChronicallyAbsent{self.fy_minus_2}Grade6],[PctChronicallyAbsent{self.previous_fiscal_year}Grade6]
          ,[PctChronicallyAbsent{self.fiscal_year}Grade6]
          ,[PctChronicallyAbsent{self.fy_minus_3}Grade7],[PctChronicallyAbsent{self.fy_minus_2}Grade7]
          ,[PctChronicallyAbsent{self.previous_fiscal_year}Grade7]
          ,[PctChronicallyAbsent{self.fiscal_year}Grade7],[PctChronicallyAbsent{self.fy_minus_3}Grade8]
          ,[PctChronicallyAbsent{self.fy_minus_2}Grade8]
          ,[PctChronicallyAbsent{self.previous_fiscal_year}Grade8],[PctChronicallyAbsent{self.fiscal_year}Grade8]
          ,[PctChronicallyAbsent{self.fy_minus_3}All]
          ,[PctChronicallyAbsent{self.fy_minus_2}All],[PctChronicallyAbsent{self.previous_fiscal_year}All]
          ,[PctChronicallyAbsent{self.fiscal_year}All]'''
        names = names.split(',')
        names = pd.Series(names)
        names = names.str.replace('[','', regex=True).str.replace(']','', regex=True).str.strip()
        return names

    def get_csi_el_drilldown_columns(self):
        names='''[FederalModel]
      ,[EntityID]
      ,[FiscalYear]
      ,[K8TotalNumberELFayStudents]
      ,[K8NumberofProficientStudents]
      ,[K8TotalNumberTested]
      ,[K8PercentProficient]
      ,[K8StatewidePcttProf]
      ,[K8StateWideSTDProf]
      ,[K8NumofStudentsImp1ProfLevels]
      ,[K8NumofStudentsImp2ProfLevels]
      ,[K8NumofStudentsImp3ProfLevels]
      ,[K8ELTotalNumberTested]
      ,[K8PercentGrowth]
      ,[K8StatewidePctGrowth]
      ,[K8StateWideSTDGrowth]
      ,[912TotalNumberELFayStudents]
      ,[912NumberofProficientStudents]
      ,[912TotalNumberTested]
      ,[912PercentProficient]
      ,[912StatewidePcttProf]
      ,[912StateWideSTDProf]
      ,[912NumofStudentsImp1ProfLevels]
      ,[912NumofStudentsImp2ProfLevels]
      ,[912NumofStudentsImp3ProfLevels]
      ,[912ELTotalNumberTested]
      ,[912PercentGrowth]
      ,[912StatewidePctGrowth]
      ,[912StateWideSTDGrowth]'''
        names = names.split(',')
        names = pd.Series(names)
        names = names.str.replace('[','', regex=True).str.replace(']','', regex=True).str.strip()
        return names
        
    def get_csi_gr_drilldown_columns(self):
        names=f'''[FederalModel],[EntityID],[FiscalYear],[WhiteNG],[WhiteNC],[WhiteGR],[AfricanAmericanNG],[AfricanAmericanNC],[AfricanAmericanGR],[HispanicLatinoNG],[HispanicLatinoNC],[HispanicLatinoGR],[AsianNG],[AsianNC],[AsianGR],[NativeAmericanNG],[NativeAmericanNC],[NativeAmericanGR],[PacificIslanderNG],[PacificIslanderNC],[PacificIslanderGR],[TwoorMoreRacesNG],[TwoorMoreRacesNC],[TwoorMoreRacesGR],[ELFEP14NG],[ELFEP14NC],[ELFEP14GR],[SWDNG],[SWDNC],[SWDGR],[IE12NG],[IE12NC],[IE12GR],[AllNG],[AllNC],[AllGR],[WhiteCohort{self.fy_minus_4}GR],[WhiteCohort{self.fy_minus_3}GR],[WhiteCohort{self.fy_minus_2}GR],[WhiteCohort{self.previous_fiscal_year}GR],[AfricanAmericanCohort{self.fy_minus_4}GR],[AfricanAmericanCohort{self.fy_minus_3}GR],[AfricanAmericanCohort{self.fy_minus_2}GR],[AfricanAmericanCohort{self.previous_fiscal_year}GR],[HispanicLatinoCohort{self.fy_minus_4}GR],[HispanicLatinoCohort{self.fy_minus_3}GR],[HispanicLatinoCohort{self.fy_minus_2}GR],[HispanicLatinoCohort{self.previous_fiscal_year}GR],[AsianCohort{self.fy_minus_4}GR],[AsianCohort{self.fy_minus_3}GR],[AsianCohort{self.fy_minus_2}GR],[AsianCohort{self.previous_fiscal_year}GR],[NativeAmericanCohort{self.fy_minus_4}GR],[NativeAmericanCohort{self.fy_minus_3}GR],[NativeAmericanCohort{self.fy_minus_2}GR],[NativeAmericanCohort{self.previous_fiscal_year}GR],[PacificIslanderCohort{self.fy_minus_4}GR],[PacificIslanderCohort{self.fy_minus_3}GR],[PacificIslanderCohort{self.fy_minus_2}GR],[PacificIslanderCohort{self.previous_fiscal_year}GR],[TwoorMoreRacesCohort{self.fy_minus_4}GR],[TwoorMoreRacesCohort{self.fy_minus_3}GR],[TwoorMoreRacesCohort{self.fy_minus_2}GR],[TwoorMoreRacesCohort{self.previous_fiscal_year}GR],[ELFEP14Cohort{self.fy_minus_4}GR],[ELFEP14Cohort{self.fy_minus_3}GR],[ELFEP14Cohort{self.fy_minus_2}GR],[ELFEP14Cohort{self.previous_fiscal_year}GR],[SWDCohort{self.fy_minus_4}GR],[SWDCohort{self.fy_minus_3}GR],[SWDCohort{self.fy_minus_2}GR],[SWDCohort{self.previous_fiscal_year}GR],[IE12Cohort{self.fy_minus_4}GR],[IE12Cohort{self.fy_minus_3}GR],[IE12Cohort{self.fy_minus_2}GR],[IE12Cohort{self.previous_fiscal_year}GR],[AllCohort{self.fy_minus_4}GR],[AllCohort{self.fy_minus_3}GR],[AllCohort{self.fy_minus_2}GR],[AllCohort{self.previous_fiscal_year}GR]'''
        names = names.split(',')
        names = pd.Series(names)
        names = names.str.replace('[','', regex=True).str.replace(']','', regex=True).str.strip()
        return names
    
    def get_csi_do_drilldown_columns(self):
        names=f'''[FederalModel],[EntityID],[FiscalYear],[WhiteNumberDO],[WhiteNumberEnrolled],[WhiteDR],[AfricanAmericanNumberDO],[AfricanAmericanNumberEnrolled],[AfricanAmericanDR],[HispanicLatinoNumberDO],[HispanicLatinoNumberEnrolled],[HispanicLatinoDR],[AsianNumberDO],[AsianNumberEnrolled],[AsianDR],[NativeAmericanNumberDO],[NativeAmericanNumberEnrolled],[NativeAmericanDR],[PacificIslanderNumberDO],[PacificIslanderNumberEnrolled],[PacificIslanderDR],[TwoorMoreRacesNumberDO],[TwoorMoreRacesNumberEnrolled],[TwoorMoreRacesDR],[ELFEP14NumberDO],[ELFEP14NumberEnrolled],[ELFEP14DR],[SWDNumberDO],[SWDNumberEnrolled],[SWDDR],[IE12NumberDO],[IE12NumberEnrolled],[IE12DR],[AllNumberDO],[AllNumberEnrolled],[AllDR],[WhiteFY{self.fy_minus_3}DR],[WhiteFY{self.fy_minus_2}DR],[WhiteFY{self.previous_fiscal_year}DR],[WhiteFY{self.fiscal_year}DR],[AfricanAmericanFY{self.fy_minus_3}DR],[AfricanAmericanFY{self.fy_minus_2}DR],[AfricanAmericanFY{self.previous_fiscal_year}DR],[AfricanAmericanFY{self.fiscal_year}DR],[HispanicLatinoFY{self.fy_minus_3}DR],[HispanicLatinoFY{self.fy_minus_2}DR],[HispanicLatinoFY{self.previous_fiscal_year}DR],[HispanicLatinoFY{self.fiscal_year}DR],[AsianFY{self.fy_minus_3}DR],[AsianFY{self.fy_minus_2}DR],[AsianFY{self.previous_fiscal_year}DR],[AsianFY{self.fiscal_year}DR],[NativeAmericanFY{self.fy_minus_3}DR],[NativeAmericanFY{self.fy_minus_2}DR],[NativeAmericanFY{self.previous_fiscal_year}DR],[NativeAmericanFY{self.fiscal_year}DR],[PacificIslanderFY{self.fy_minus_3}DR],[PacificIslanderFY{self.fy_minus_2}DR],[PacificIslanderFY{self.previous_fiscal_year}DR],[PacificIslanderFY{self.fiscal_year}DR],[TwoorMoreRacesFY{self.fy_minus_3}DR],[TwoorMoreRacesFY{self.fy_minus_2}DR],[TwoorMoreRacesFY{self.previous_fiscal_year}DR],[TwoorMoreRacesFY{self.fiscal_year}DR],[ELFEP14FY{self.fy_minus_3}DR],[ELFEP14FY{self.fy_minus_2}DR],[ELFEP14FY{self.previous_fiscal_year}DR],[ELFEP14FY{self.fiscal_year}DR],[SWDFY{self.fy_minus_3}DR],[SWDFY{self.fy_minus_2}DR],[SWDFY{self.previous_fiscal_year}DR],[SWDFY{self.fiscal_year}DR],[IE12FY{self.fy_minus_3}DR],[IE12FY{self.fy_minus_2}DR],[IE12FY{self.previous_fiscal_year}DR],[IE12FY{self.fiscal_year}DR],[AllFY{self.fy_minus_3}DR],[AllFY{self.fy_minus_2}DR],[AllFY{self.previous_fiscal_year}DR],[AllFY{self.fiscal_year}DR]'''
        names = names.split(',')
        names = pd.Series(names)
        names = names.str.replace('[','', regex=True).str.replace(']','', regex=True).str.strip()
        return names
    
    
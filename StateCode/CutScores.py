# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 12:35:12 2023

@author: YFahmy
"""

from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
from dash.dash_table.Format import Format, Scheme, Sign


class CutScores(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        
        ## define a list of cols that must be numeric for this module to succeed
        self.numeric_cols = ['EL', 'ELFAY', 'SAISID', 'EntityID', 'StudentGrade', 'ELProf', 'ELGrowth', 'ELTested']
        ## rename cols in staticfile in case we use the old staticfile naming conventions
        self.ineligible_cuts = {'Alternative':60
                                  ,'912':50
                                  ,'K8':80}
        self.cuts_py = {'Alternative': {'A': [130, 83.01],
                                  'B': [83, 65.01],
                                  'C': [65, 47.01],
                                  'D': [47, 29.01],
                                  'F': [29, 0]},
                         '912': {'A': [130, 82.01],
                                  'B': [82, 65.01],
                                  'C': [65, 48.01],
                                  'D': [48, 31.01],
                                  'F': [31, 0]},
                         'K8': {'A': [130, 84.67],
                              'B': [84.66, 72.39],
                              'C': [72.38, 60.11],
                              'D': [60.10, 47.83],
                              'F': [47.82, 0]}}
        self.py_cuts_floored = {'Alternative': {'A': [130, 83.01],
                                  'B': [83, 65.01],
                                  'C': [65, 47.01],
                                  'D': [47, 29.01],
                                  'F': [29, 0]},
                             '912': {'A': [130, 82],
                                      'B': [81.99, 65],
                                      'C': [64.99, 48],
                                      'D': [47.99, 31],
                                      'F': [30.99, 0]},
                             'K8': {'A': [130, 84],
                                  'B': [83.99, 72],
                                  'C': [71.99, 60],
                                  'D': [59.99, 47],
                                  'F': [46.99, 0]}}
        self.generic_cuts = {'A': [130, 90],
                              'B': [89.99, 80],
                              'C': [79.99, 70],
                              'D': [69.99, 60],
                              'F': [59.99, 0]}
        
        
    def calculate_component(self, staticfile_raw, schooltype):
        data, schooltype = self.get_data()
        data_22 = self.get_data(py_data=True)
        
        data_cln, schooltype = self.process_cy_data(data, schooltype)
        grades_py = self.process_py_data(data_22)
        
        # #------------------- Assign grades based on SD cuts
        sd_cuts = self.get_sd_cuts(data_cln)
        grades_sd_cuts = self.assign_grades(data_cln, sd_cuts)
        
        # #------------------ Assign grades based on PY cuts
        grades_py_cuts = self.assign_grades(data_cln, self.py_cuts_floored)
        
        # #----------------- Assign Grades based on 90,80,70 cuts
        # models = ['Alternative', '912', 'K8']
        norm_cuts = {m:self.generic_cuts for m in grades_py_cuts.keys()}
        grades_norm_cuts = self.assign_grades(data_cln, norm_cuts)
        
        ## generate the cuts table
        table = self.generate_cuts_table( sd_cuts, norm_cuts)
        
        ## put the data in long format
        long_data = {}
        for i in grades_sd_cuts.keys():
            datasets = [grades_norm_cuts[i].copy(), grades_py_cuts[i].copy(), grades_sd_cuts[i].copy(), grades_py[i].copy()]
            method_cuts = ['90-80-70-60 Cuts', 'PY Cuts', 'SD Cuts', f'{self.fiscal_year-1} Grades']
            for df,method in zip(datasets, method_cuts):
                df['Method'] = method
            long_data[i] = pd.concat(datasets, axis=0 )
        ## make the main 3 plots
        cuts_dist_overlayed = self.generate_cuts_counts(long_data)
        cuts_side_by_side = self.generate_faceted_cuts_counts(long_data)
        lolipop= self.generate_differential_analysis(long_data)
        grades_22_23 = self.generate_parrallel_categories( grades_norm_cuts, grades_py_cuts, grades_sd_cuts, grades_py)
            
    def generate_cuts_table(self, sd_cuts, norm_cuts):
        #put cuts in a plotly table format
        cuts_all = {}
        for m in norm_cuts.keys():
            data = pd.DataFrame()
            for d, cut in zip([ self.py_cuts_floored[m].copy(),  sd_cuts[m].copy(), norm_cuts[m].copy()], ['PY', 'SD', '90-80-70']):
                d['A'][0] = 100
                d = {grade: f'{g_range[0]} : {g_range[1]}' for grade, g_range in d.items()}
                temp = pd.DataFrame(d, index=[0])
                temp['Method'] = cut
                data = pd.concat([data, temp], axis=0)
                cuts_all[m] = data
                
        #plot cuts in plotly table
        table = {}
        for m in cuts_all.keys():
            cuts = cuts_all[m]
            print(m)
            table[m] = go.Figure(data=[go.Table(
                                            header=dict(values=list(['Method', 'A', 'B', 'C', 'D', 'F'])
                                                       ,fill_color='grey'
                                                        ,line_color='darkslategray'
                                                        ,align='center'
                                                       ,font=dict(color='black', size=18, family="Arial Black")
                                                       ,height=40)
                                            ,cells=dict(values=[cuts.Method, cuts.A, cuts.B, cuts.C, cuts.D, cuts.F]
                                                       ,font = dict(color = 'darkslategray', size = 14, family="Arial Black")
                                                       ,fill_color = [['white','lightgrey','white','lightgrey','white']*4]
                                                       ,height=40))
                                     ])
            table[m].show()
        return table
    
    def generate_cuts_counts(self, long_data):
        # # Generate a plot to compare cuts distributions accross the 3 methods using ****** PLOTLY
        cuts_dist_overlayed={}
        for m in long_data.keys():
            plt.figure(figsize=(20,10))
            long_data[m].sort_values(['Method', 'Grades'], inplace=True)
            cuts_dist_overlayed[m] = px.histogram(long_data[m], x='Grades', color='Method'
                         , barmode='group', title=m+ ' Grades Distribution', text_auto=True
                        , color_discrete_sequence= ['black', "maroon","purple","coral"])
            cuts_dist_overlayed[m].update_traces( textposition='outside')
            cuts_dist_overlayed[m].update_layout(font=dict(
                                        family="Arial Black",
                                        size=15,
                                        color="black")
                             , title = {'xanchor': 'center',
                                            'yanchor': 'top',
                                       'x':0.5
                                       ,'y':1}
                             ,plot_bgcolor='white')
            for n in np.arange(0.5,4.5):
                cuts_dist_overlayed[m].add_vline(x=n, line_width=1, line_dash="dot", line_color="grey")
            cuts_dist_overlayed[m].update_yaxes(ticks='outside',
                            showline=True,
                            linecolor='black',
                            gridcolor='lightgrey'
                        )
            cuts_dist_overlayed[m].show()
        return cuts_dist_overlayed
            
    def generate_faceted_cuts_counts(self, long_data):
        # # Generate a plot to compare cuts distributions accross the 3 methods using ****** PLOTLY
        cuts_side_by_side={}
        for m in long_data.keys():
            plt.figure(figsize=(20,10))
            long_data[m].sort_values(['Method', 'Grades'], inplace=True)
            cuts_side_by_side[m] = px.histogram(long_data[m], x='Grades', facet_col ='Method', title=m+' Cuts', color='Method'
                                                , color_discrete_sequence= ['black', "maroon","purple","coral"]
                                               , text_auto=True)
        
            
            cuts_side_by_side[m].update_traces( textposition='outside')
            cuts_side_by_side[m].update_layout(font=dict(
                                        family="Arial Black",
                                        size=15,
                                        color="black")
                             , title = {'xanchor': 'center',
                                            'yanchor': 'top',
                                       'x':0.5
                                       ,'y':1}
                             ,plot_bgcolor='white')
            cuts_side_by_side[m].update_yaxes(ticks='outside',
                            showline=True,
                            linecolor='black',
                            gridcolor='lightgrey'
                        )
            for a in cuts_side_by_side[m].layout.annotations:
                a.text = a.text.split("=")[1]
            cuts_side_by_side[m].show()
        return cuts_side_by_side
            
    def generate_differential_analysis(self, long_data):
        ##------------------------------------calculate the differential data
        diff_data={}
        for m, df in long_data.items():
            difference = pd.DataFrame()
            for sub in ['Charter', 'AOI']:
                temp = df[~df.Method.isin([f'{self.fiscal_year-1} Grades'])].copy()
                temp.replace({'AOI':{1:'AOI'
                                    ,0:'Brick & Morter'}
                             ,'Charter':{1:'Charter'
                                        ,0:'District'}}, inplace=True)
                diff = temp.groupby(['Method',sub,'Grades']).agg(SchoolCount = ('EntityID','nunique')).reset_index()
                diff.rename({sub:'School Type'}, axis=1, inplace=True)
                
                #get totals
                totals = temp.groupby(['Method']).agg(TotalCount = ('EntityID','nunique')).reset_index()
                
                #merge totals to dif
                diff = pd.merge(diff, totals, on='Method', suffixes=('', ' Totals'))
                diff = pd.pivot(diff, index=['Grades', 'School Type'], values=['SchoolCount', 'TotalCount']
                                      , columns=['Method']).reset_index()
                diff.columns = [i[1]+i[0] for i in diff.columns]
                difference = pd.concat([difference, diff], axis=0)
            ##calculate the differnces
            difference['PCT Gain (PY to SD)'] = ((difference['PY CutsSchoolCount'] - difference['SD CutsSchoolCount'])*100/ difference['PY CutsTotalCount']).round(1)
            difference['PCT Gain (PY to 90-80-70)'] = ((difference['PY CutsSchoolCount'] - difference['90-80-70-60 CutsSchoolCount'])*100/ difference['PY CutsTotalCount']).round(1)
            
            #add final data
            diff_data[m] = difference
            
        
        ##-------------------------------------plot the chart
        lolipop={}
        for m, df in diff_data.items():
            for type_ in [['Brick & Morter', 'AOI'], ['District', 'Charter']]:
                ##
                if type_[1] == 'Charter':
                    color_discrete_map = {'Charter':'palevioletred'
                                         ,'District':'teal'}
                else:
                    color_discrete_map=None
                    
                for sm in ['SD', '90-80-70']:
                    ##
                    if sm == 'SD':
                        pct_gain = 'PCT Gain (PY to SD)'
                    else:
                        pct_gain = 'PCT Gain (PY to 90-80-70)'
                    
                        
                    temp = df[df['School Type'].isin(type_)].copy()
                    temp = temp [(temp[pct_gain].notnull()) ]
                    temp = temp [(temp[pct_gain]!=0) ]
                    temp = temp.rename({pct_gain:'PCT Gain'}, axis=1)
                    
                    lolipop[m+type_[1]+sm] = px.bar(temp, x='PCT Gain', color='School Type', y='Grades'
                                        ,barmode='group', title=m+f': {sm} -- PY'+ ' Cuts Impact', color_discrete_map=color_discrete_map
                                       ,category_orders={'Grades':['A', 'B', 'C', 'D','F']})
                    lolipop[m+type_[1]+sm].for_each_trace(lambda trace: trace.update(text=[float(x) for x in list(abs(trace.x))]))
                    for t,l, c in zip([f"<b>{sm} Cuts", '<b>PY Cuts'], [0,1], ["coral","purple"]):
                        if sm == '90-80-70' and l==0:
                            c='maroon'
                        lolipop[m+type_[1]+sm].add_annotation(
                                                text=t,
                                                xref="x domain",
                                                yref="y domain",
                                                x=l, y=-0.05,
                                                showarrow=False,
                                                font={'size':30
                                                     ,'color':c
                                                     ,'family':'Arial'})
                    lolipop[m+type_[1]+sm].update_traces( textposition='auto')
                    lolipop[m+type_[1]+sm].update_layout(font=dict(
                                                        family="Arial Black",
                                                        size=15,
                                                        color="black")
                                             , title = {'xanchor': 'center',
                                                        'yanchor': 'top',
                                                       'x':0.5
                                                       ,'y':1
                                                       ,'font':{'size':30}}
                                             ,plot_bgcolor='ghostwhite'
        
                                            ,bargap=0.5
                                            ,yaxis = dict( tickfont = dict(size=20))
                                            ,yaxis_title=None
                                            )
                    lolipop[m+type_[1]+sm].update_yaxes(ticks='inside',
                                showline=False,
        
                                 )
                    lolipop[m+type_[1]+sm].update_xaxes(tickfont_size=1,
                                           zeroline=True,
                                            zerolinecolor='black',
                                            zerolinewidth=2,
                                            mirror=True,
                                            showline=True)
                    for n in np.arange(0.5,4.5):
                        lolipop[m+type_[1]+sm].add_hline(y=n, line_width=1, line_dash="dot", line_color="grey")
                    lolipop[m+type_[1]+sm].show()
        return lolipop
    
    def generate_parrallel_categories(self, grades_norm_cuts, grades_py_cuts, grades_sd_cuts, grades_py):  
        #merge 22 to 2023 data
        combined = {}
        for i in grades_sd_cuts.keys():
            g22 = grades_py[i].rename({'Grades':'Grades22'}, axis=1)
            g23py =grades_py_cuts[i].rename({'Grades':'Grades23_PYCuts'}, axis=1)
            g23sd = grades_sd_cuts[i].rename({'Grades':'Grades23_SDCuts'}, axis=1)
            g23norm = grades_norm_cuts[i].rename({'Grades':'Grades23_NormCuts'}, axis=1)
            x = pd.merge(g22, g23py , on='EntityID', suffixes=('','_dup'))
            y = pd.merge(x, g23sd, on='EntityID', suffixes=('','_dup'))
            combined[i]= pd.merge(y, g23norm, on='EntityID', suffixes=('','_dup'))
            
        grades = {'A':'blue'
         ,'B':'mediumseagreen'
         ,'C':'yellow'
         ,'D':'orange'
         ,'F':'red'}
        grades_22_23 = {}
        for col, short in zip(['Grades23_SDCuts', 'Grades23_PYCuts', 'Grades23_NormCuts'], ['SD', 'PY', '90-80-70']):
            for m, df in combined.items():
                df.sort_values([col, 'Grades22'], inplace=True)
                df['GradeColor'] = df[col].map(grades)
                grades_22_23[m+short] = px.parallel_categories(df,color='GradeColor' , labels={col:'Grades 2023'
                                                                         ,'Grades22': 'Grades 2022'}
                                             ,dimensions=[ 'Grades22',col]
                                            ,title= m+ f' <br>2022 Grades => {short} Cuts'
                                            )
        
                grades_22_23[m+short].update_layout(font=dict(
                                            family="Arial Black",
                                            size=15,
                                            color="black")
                                 , title = {'xanchor': 'center',
                                                'yanchor': 'top',
                                           'x':0.5
                                           ,'y':0.95}
                                 ,plot_bgcolor='white')
                grades_22_23[m+short].update_yaxes(ticks='outside',
                                showline=True,
                                linecolor='black',
                                gridcolor='lightgrey'
                            )
                grades_22_23[m+short].show()
        return grades_22_23
        
    def process_cy_data(self, data, schooltype):
        ##change all non-typicals to two entries one of k8 and another of 912
        #create k8 entry
        mask = schooltype.StateModel=='Non-Typical'
        hybrids = schooltype[mask].copy()
        hybrids['StateModel']= 'K8'
        #alter non-typical entry to 912
        schooltype.loc[mask, 'StateModel'] = '912'
        #add hybrid k8s to schooltype
        schooltype = pd.concat([schooltype, hybrids], axis=0)
        
        ## selec relevant cols only
        ## separate hybrids from k8 and 9-12
        for model, df in data.items():
            cols = ['EntityID', 'PercentageEarned', 'TotalBonusPoints', 'TotalPointsEligible', 'TotalpointsEligibleHybridModel']
            df = df.loc[:, df.columns.isin(cols)].copy()
            #rename col
            if model == 'Non-Typical':
                df.rename({'TotalpointsEligibleHybridModel':'TotalPointsEligible'}, axis=1, inplace=True)
            ##convert cols to numeric
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            # filter data to relevant models only
            data[model] = pd.merge(df, schooltype.loc[schooltype.StateModel==model], left_on='EntityID', right_on='SchoolCode')
            
        ## remove schools that don't qualify based on points eligible cutoff    
        data_cln = {}
        for m, df in data.items():
            data_cln[m] = df[df.TotalPointsEligible >= self.ineligible_cuts[m]].copy()
            print(m, data_cln[m].shape)
            
        return data_cln, schooltype
            
    #Method will need update since table structure in 22 is different from 23
    def process_py_data(self, data_22):
        
        # clean data and filter cols
        data_22_cln={}
        for m, df in data_22.items():
            #keep only relevant cols
            temp = df[['EntityID','TotalPointsEligible', 'PercentageEarned']].copy()
            #remove % signs
            temp['PercentageEarned'] = temp['PercentageEarned'].astype(str).str.replace('%','')
            #convert cols to numeric
            for i in ['EntityID','TotalPointsEligible', 'PercentageEarned']:
                temp[i] = pd.to_numeric(temp[i], errors='coerce')
            #save data
            data_22_cln[m] = temp.copy()
        
        ## remove schools that don't qualify based on points eligible cutoff    
        for m, df in data_22_cln.items():
            data_22_cln[m] = df[df.TotalPointsEligible >= self.ineligible_cuts[m]].copy()
            print(m, data_22_cln[m].shape)
            
        ## assign letter grades based on last years cuts
        grades_py = self.assign_grades(data_22_cln, self.cuts_py)
        return grades_py
            
        
    
    # ## bring in the data from sql server
    def get_data(self, py_data=False):
        if py_data:
            fiscal_year = self.fiscal_year-1
        else:
            fiscal_year = self.fiscal_year
        
        db = DATABASE(fiscal_year=fiscal_year, database='REDATA_UAT', schema='grading', run=self.run, server_name='AACTASTPDDBVM02')
        #setup a dict with model-table name
        sources = {'Alternative':'ALTSummary9Thru12'
                  ,'912':'Summary9Thru12'
                  ,'K8':'SummaryKThru8'}
    
        data = {}
        for model, table in sources.items():
            data[model] =  db.read_table(table_name=table, suffix_fy=True, prefix_run=False)
        
        if py_data is False:
            ##bring in schooltype file
            sql = f'Select * FROM [AccountabilityArchive].[Static].[{self.run}SchoolType{self.fiscal_year}]'
            schooltype = db.read_sql_query(sql)
            return data, schooltype
        else:
            return data

    
    def get_sd_cuts(self, data_cln):
        cutscores = {}
        for m,df in data_cln.items():
            #calculate mean and std based on dist without outliers
            mask_mean = df.PercentageEarned>0
            dist = df.loc[mask_mean, 'PercentageEarned']
            q1 = np.percentile(dist, 25, interpolation = 'midpoint')
            q3 = np.percentile(dist, 75, interpolation = 'midpoint')
            iqr_out = (q3 - q1) * 1.5
            #remove outliers before mean calc
            no_outliers = dist[(dist > q1-iqr_out) & (dist < q3+iqr_out)]
            mean = no_outliers.mean()
            sd = no_outliers.std()
            
            cuts ={'b_lower' : np.floor(mean)
                    ,'c_lower' : np.floor(mean-sd)
                    ,'b_upper' : np.floor(mean+sd)
                    ,'d_lower' :np.floor(mean-(2*sd))}
            f = 0.01
            cs = {'A':[130, cuts['b_upper']]
                ,'B':[cuts['b_upper']-f, cuts['b_lower'] ]
                ,'C': [cuts['b_lower']-f, cuts['c_lower'] ]
                ,'D': [cuts['c_lower']-f, cuts['d_lower'] ]
                ,'F': [cuts['d_lower']-f, 0 ]}
            cutscores[m] = cs
        return cutscores
    
    def plot_distributions(self, data_cln, *cuts):
        for m,df in data_cln.items():
            df.PercentageEarned.plot.hist(title=m, bins=40)
            for j in cuts:
                for i in j[m].values():
                    plt.axvline(i[1], linewidth=2, color='r')
            plt.show()
            
            
    def assign_grades(self, data_cln, cuts):
        output = {}
        #iterate through the models data
        for m, x in data_cln.items():
            df = x.copy()
            ## if Pct earned falls within cuts, then fill grade
            for grade, bounds in cuts[m].items():
                df.loc[df.PercentageEarned.between(bounds[1], bounds[0]), 'Grades'] = grade
            output[m] = df
        return output
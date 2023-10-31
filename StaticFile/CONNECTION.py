# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 13:13:51 2023

@author: yfahmy
"""
import pyodbc 

class CONNECTION:
    '''setup a connection to the database of your choice over a server of your choice
    This class is set to be flexible for user needs
    to connect to DB using a different module other than pyodbc 
    simply set up a method that connects to DB using your preffered connection and return connection object 'cnxn'
    Then call your method under the __call__ function insted of the current method
    '''
    
    def __init__(self):
        pass
        
    def __call__(self, server_name):
        cnxn = self.connect_with_pyodbc(server_name = server_name)
        return cnxn
    
    def  connect_with_pyodbc(self, server_name):
        ''' setup a connection to the database of your choice over a server of your choice
        Args:
            server_name (str): the server name to connect to, 
            scheme      (str):The database schema to connect to
        Returns:
            connection object 'cnxn'
        '''
        # setup a connection using pyODBC
        cnxn_str = ('''Driver=ODBC Driver 17 for SQL Server;
                    Server={a};
                    Trusted_Connection=yes;'''.format(a = server_name))
        cnxn = pyodbc.connect(cnxn_str)
        
        return cnxn
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 09:57:33 2025

@author: jkinser


### Edited version for CDS465 Group 3 Project: Modeling Mining and Manufacturing Processes
### Editor: Eric Wu
### Date: 10.29.2025
### Version: b.1 
"""

#mydir = '/home/jkinser/Documents/courses/CDSCardinal/Version4/'
#mydir = '/Users/jkinser/Documents/CoursesKinser/CDSCardinal/Version3/'
mydir = '/Users/eric/Documents/CDS465/'
pysrc = mydir + 'pysrc/'
datadir = mydir + 'data/'
popdir = mydir + 'population/'

#%%
import numpy as np
import pandas as pd
import sqlite3 as sql
import sys
sys.path.append( pysrc )
#
import names, island, population, movies, human, nomad 

sys.path.append('/Users/eric/Documents/CDS465/archieve/Dev_P1/') #Location of initialization.py
import initialization as initf 
import sqlaccess, firstmigration

#%%
class Realm:
    def __init__(self, datadir):
        # Load DNA cursor
        dnaname = 'dnastats.db'
        dnaconn = sql.connect(datadir + dnaname )
        self.dnacur = dnaconn.cursor()
        act = 'SELECT COUNT(*) FROM main'
        self.dnacur.execute( act )
        # load names
        self.bnames, self.gnames, self.lnames = names.LoadNames( datadir )
        # load island
        self.isle = island.LoadIsland( datadir + 'island.pickle')
        #self.isle = island.InitCardinalIsland( datadir + 'island.png')
        # Empty DFs
        population.EmptyDFs( self )
        movies.CreateMoviesDFs( self, datadir )
        self.usemovies= False # turn on when ready to use movies
        # params
        self.marriageradius = False # radius is not restricted

        ###START CHANGES P1
        self.folderpath: str = None #Store folderpath
        self.loc_gen_dict: dict = None #Store model params
        self.mm_debt_thold = -10000 #Store model params
        self.accounting_period = 3 #Store model accounting period param

    def InitializeMM(self):
        """
        Calls functions from initialization.py to populate mm_location_master and tables containing .location_coord
        """
        self.mm_dfs = initf.initialize_mm(self, self.folderpath,self.loc_gen_dict)
        
    def initfQuickLook(self, n: int = 5):
        """Prints the head of each dataframe, given n the number of records to show"""
        initf.quick_look(self.mm_dfs, n)

    def initfSimulateOrders(self, ordersdf: pd.DataFrame = None) -> pd.DataFrame:
        """
        This function simulates the B2B sales between manufacturers and customers.

        inpts:
            stores_orders_df (pd.DataFrame): the qa_sandbox_orders df

        opts:
            ordersdf (pd.DataFrame): the qa_sandbox_orders dataframe
        """
        return initf.simulate_orders(self, self.folderpath, ordersdf)
    
    def initfCalcCostProfit(self, ordersdf: pd.DataFrame = None) -> pd.DataFrame:
        """This function calculates the COGS and profit generated per order.

        inpts:
            mm_dataframes (dict): A dictionary of the MM DataFrames; the keys are the names of the DataFrames and the values are the DataFrames
            qa_sandbox_orders: the qa_sandbox_orders dataframe

        opts: 
            a dataframe of the costs and profits given an order
        """
        return initf.calc_cost_profit(self, self.mm_dfs, ordersdf)

    def InitializePopulation( self, N=500, month=1000, dometh=0 ):
        population.InitialPopulation( self, N, month, dometh )
        
    def AddImmigrants(self, N, dometh):
        # random people
        nomad.Immigrate( self, N, dometh )
    def Evolve(self, Nmonths, addpeep, dometh, migrate=False ):
        for i in range( Nmonths ):
            self.month += 1
            population.OneMonth( self, dometh, migrate=migrate)
            if addpeep>0:
                nomad.Immigrate( self, addpeep, dometh )

    def EvolveMM(self, Nmonths, addpeep, dometh, migrate=False):
        """The Realm.Evolve function that has been revamped to model MM processes"""
        for i in range( Nmonths ):
            #stuff before evolve

            self.Evolve(self, 1, addpeep, dometh, migrate)

            #stuff after evolve

    def InitialMigration( self, months, locs, pmrange, pct ):
        firstmigration.FirstMigration( self, months, locs, pmrange, pct )
    def ReadAll( self, fname ):
        conn = sql.connect( fname )
        self.persondf = pd.read_sql( 'SELECT * FROM person', conn, index_col='index' ) 
        self.jaildf = pd.read_sql( 'SELECT * FROM jail', conn, index_col='index' ) 
        self.weddf = pd.read_sql( 'SELECT * FROM wed', conn, index_col='index' )
        self.hospdf = pd.read_sql('SELECT * FROM hospital', conn, index_col='index' )
        self.portdf = pd.read_sql('SELECT * FROM port', conn, index_col='index' )
        self.missingdf = pd.read_sql('SELECT * FROM missing', conn, index_col='index' )
        self.moviesdf = pd.read_sql( 'SELECT * FROM movies', conn, index_col='index' )
        self.inmoviedf = pd.read_sql( 'SELECT * FROM inmovie', conn, index_col='index' )
        cur = conn.cursor()
        self.month = cur.execute( 'SELECT * FROM clock ').fetchone()[0]
        # storing empty DFs may cause dtype changes
        dct =  {'husband':int, 'wife':int, 'date':int, 'divorce':int, 'spdie':int}
        self.weddf = self.weddf.astype( dct )

    def SaveAll( self, fname ):
        conn = sql.connect( fname )
        cur = conn.cursor()
        act = 'DROP TABlE IF EXISTS clock'
        cur.execute( act )
        sqlaccess.CreateClock( conn, cur, self.month )
        self.persondf.to_sql( 'person', conn, if_exists='replace')
        self.jaildf.to_sql( 'jail', conn, if_exists='replace')
        self.weddf.to_sql( 'wed', conn, if_exists='replace')
        self.hospdf.to_sql( 'hospital', conn, if_exists='replace')
        self.portdf.to_sql( 'port', conn, if_exists='replace')
        self.missingdf.to_sql( 'missing', conn, if_exists='replace')
        self.moviesdf.to_sql( 'movies', conn, if_exists='replace')
        self.inmoviedf.to_sql( 'inmovie', conn,  if_exists='replace')
        conn.commit()
    
        
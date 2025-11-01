"""
Author: Eric Wu
Date: 10.31.2025
Version: b.1

This version of operations.py is designed to run in python 3.12.x 

Copyright (C) 2025 Eric Wu. All rights reserved.
"""
###import dependencies
import numpy as np 
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=FutureWarning) #Ignore future warnings. Code runs as intended on pythong 3.12.x

#Pre Evolve Functions
def check_solvency(realmc) -> bool:
    """
    This function should check whether the location is or is not a going concern, given a threshold that businesses are able to go down to.

    ***PHASE2: different thresholds for different subdomains

    inpts:
        Realmc (class Realm obj): E

    opts:
        Returns True if all locations are solvent;
            False if any location is insolvent given the threshold
    """
    # if any balance is less than the debt threshold, then this function will return False, 
    #   because the number of Trues from the series will be less than the number of locations,
    #   which means a location must have had a lower balance than that of mm_debt_thold
    return False if sum(realmc.mm_dfs['mm_location_master']['balance'] > realmc.mm_debt_thold) < len(realmc.mm_dfs['mm_location_master']) else False

def recieve_orders(realmc, ordersdf: pd.DataFrame):
    """
    This function takes the orders recieved and adds them to mm_order_master

    inpts: 
        Realmc (class Realm obj): E,
        ordersdf (pd.DataFrame): the DataFrame of orders from the stores; should follow the data standards of mm_order_master
    """

    realmc.mm_dfs['mm_order_master'] = pd.concat([realmc.mm_dfs['mm_order_master'], ordersdf],ignore_index=True).drop_duplicates()

def check_order_parity(realmc, ordersdf: pd.DataFrame):
    """
    This function checks that all of the orders recieved this month where added to mm_order_master.
        If orders are missing, add them into mm_order_master
    """
    pass

def preopsMM(realmc, ordersdf: pd.DataFrame):
    """
    This function houses the processes for MM operations that run before the month is updated, before evolve
    """
    if check_solvency(realmc):
        recieve_orders(realmc, ordersdf)
        check_order_parity(realmc, ordersdf)
    else:
        print(f'This location is insolvent {realmc.mm_dfs['mm_location_master'].iloc[np.argwhere(check_solvency(realmc))]['location_coord']} at month {realmc.month}') ###NOTE: Check here; may not work.
    
#Post Evolve Functions
def mm_operations(realmc, ccpdf):
    """
    This function should run the production process workflow.
        -Calculate which products were ordered, manufactured, and fufiled:
        -Mark order statuses accordingly.
            -mm_order_master.order_status is defined as {-1: 'order_rejected', 0: 'order_complete', 1: 'order_in_production(1m)',
                2: 'order_in_production(2m)', 3: 'order_in_production(3m)', 4: 'order_in_production(4m)'}
            -any order that is [0,4] is assumed accepted.
    """
    def manufacture(realmc, ccpdf):
        """
        Manf orders; changes statuses. Passes orderes where costs and profits are recognized into update_books
        """
        pass
    def update_books(realmc, fufiled_orders):
        """
        Recognizes costs and profits of orders given the orders that have been fufiled; 
            updates mm_location_master.balances and mm_books as appropriate;
            Starts a new entry in mm_books when realmc.month = 1 + 3*i.
        """
        pass

    fufiled_orders = manufacture(realmc, ccpdf)
    update_books(realmc, fufiled_orders)

def mm_tax(realmc, ccpdf: pd.DataFrame):
    """
    This function checks the month to determine the end of an accounting period;
        if it is the end of an accounting period, then taxes are paid accordingly.
    """
    def check_tax(realmc) -> bool:
        """
        This function checks if realmc.month if the end of an accounting period;
            if realmc.month % 3 == 0, then return true, else false
        """
        return True if realmc.month % realmc.accounting_period == 0 else False
        
    def pay_tax(realmc, ccpdf: pd.DataFrame):
        """
        Define tax type rate as GLOBAL var.
        This function calculates the sales tax and corperate income tax collected during the accounting period (realmc.accounting_period);
            The funds are to be taken from balance to pay for CIT.
        """

        end_month = realmc.month
        begin_month = end_month - realmc.accounting_period + 1
        
        q1 = ccpdf['order_date'] >= begin_month 
        q2 = ccpdf['order_date'] <= end_month
        #q3 = 

        #ccpdf_period = ccpdf.loc[query] 
        #sales_tax = sum(ccpdf_period['sales_tax'])
        #cit = sum(ccpdf_period['profit']-) #compute salaries as well as part of net profit 

    if check_tax(realmc):
        pay_tax(realmc, ccpdf)

def mm_hr(realmc):
    """
    This function deals with ensuring all employees are current; if they are not, people will be hired to replace them;
        this function also pays employees
    """
    def check_employees(realmc):
        """
        Ensure all employees are current; returns a list of employees who are no longer employable 
            (dead for now; distance will be considered in the future)
        """
        employee_pids = realmc.mm_dfs['mm_employee_master']['pid'].to_list()

        #check status of such employees
        q1 = realmc.persondf['pid'].isin(employee_pids)
        employees = realmc.persondf.loc[q1]

        #cont

    def hire_employees(realmc, replace_list: list):
        """
        This function recieves a list of employee pids that need to be replaced;
            this function finds new employees to hire according to the replace_list.
        """
        pass
    def pay_employees(realmc):
        """
        This function pays the employees a wage of 40,000 / 12 * (1 + np.random.rand(len(realmc.mm_dfs['mm_employee_master']['pids']))/10) 
        """
        employee_pids = realmc.mm_dfs['mm_employee_master']['pid'].to_list()
        pay = 40,000 / 12 * (1 + np.random.rand(len(employee_pids)/10))

        #check status of such employees
        q1 = realmc.persondf['pid'].isin(employee_pids)
        #realmc.persondf.loc[q1, 'savings'] += pay #add pay may or may not work, values need to be initialized.

        #need to reflect expenses in location_master and books!


    replace_list = check_employees(realmc)
    if len(replace_list) > 0:
        hire_employees(realmc)
    pay_employees(realmc)

def postopsMM(realmc, ccpdf: pd.DataFrame):
    """
    This function houses the processes for MM operations that run after the month is updated, after evolve.
    """
    mm_operations(realmc, ccpdf)
    if check_solvency(realmc):
        mm_tax(realmc, ccpdf)
        if check_solvency(realmc):
            mm_hr(realmc)
        else: 
            yield
    else: 
        yield 
        
        
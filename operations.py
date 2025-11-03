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
    return False if sum(realmc.mm_dfs['mm_location_master']['balance'] > realmc.mm_params['mm_debt_thold']) < len(realmc.mm_dfs['mm_location_master']) else False

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
        yield
    
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
        return True if realmc.month % realmc.mm_params['accounting_period'] == 0 else False ###NOTE: FIX THIS THIS LOGIC DOESN'T WORK
        
    def pay_tax(realmc, ccpdf: pd.DataFrame):
        """
        Define tax type rate as GLOBAL var.
        This function calculates the sales tax and corperate income tax collected during the accounting period (realmc.accounting_period);
            The funds are to be taken from balance to pay for CIT.
        """

        end_month = realmc.month
        begin_month = end_month - realmc.mm_params['accounting_period'] + 1
        
        q1 = ccpdf['order_date'] >= begin_month 
        q2 = ccpdf['order_date'] <= end_month
        ccpdf_period = ccpdf.loc[q1&q2] 

        #sales tax
        temp_salestax = pd.merge(realmc.mm_dfs['mm_product_master'][['product_id', 'location_coord']],ccpdf, how='right', on='product_id')
        salestax = temp_salestax.groupby('location_coord')['sales_tax'].sum().reset_index()   

        #CIT  
        q1_1 = realmc.mm_dfs['mm_books']['period_s'] == max(realmc.mm_dfs['mm_books']['period_s']) #get latest bank record
        incm = realmc.mm_dfs['mm_books'].loc[q1_1]['period_income']
        cit = 0 if incm < 1 else incm * realmc.mm_params['cit_rate']

        #subtract calculated tax

    if check_tax(realmc):
        pay_tax(realmc, ccpdf)

def mm_hr(realmc, chunk_size: int = 64):
    """
    This function deals with ensuring all employees are current; if they are not, people will be hired to replace them;
        this function also pays employees
    """
    def check_employees(realmc, radius: float) -> pd.DataFrame:
        """
        Ensure all employees are current; returns a list of employees who are no longer employable 
            (dead for now; distance will be considered in the future)
        """
        #left join employee master with realmc.persondf
        employees = pd.merge(
            left=realmc.mm_dfs['mm_employee_master'],
            right=realmc.persondf,
            how='left'
        )

        #filter out the employees who have moved away and are dead
        q1 = employees['death'] < 0 #must be alive
        evh, vh = employees[['locv', 'loch']].values, np.array(employees['location_coord'].tolist())
        d = np.linalg.norm(vh - evh, axis=1)
        q2 = d < radius #must be near location they work at d<46

        employee_replace = employees.loc[~q1|~q2]

        return employee_replace

    def hire_employees(realmc, replace: pd.DataFrame, radius: float):
        """
        This function recieves a list of employee pids that need to be replaced;
            this function finds new employees to hire according to the replace_list.
        """
        #get list of current employees 
        employee_pid = realmc.mm_dfs['mm_employee_master']['pid'].tolist()

        #get eligible workers near the loc that needs replacing
        replace = replace[['location_coord', 'pid', 'wage']]        

        cols = ['pid','lastname','death', 'locv', 'loch']
        potential_employee = realmc.persondf.query(
            f'({realmc.month} - birth) >= 18*12' \
            'and death < 0 ' \
            'and job not in [0.0,1.0]' \
            f'and pid not in {employee_pid}'
        )[cols]

        temp_df = potential_employee.assign(key=1).merge(replace.assign(key=1), on='key').drop('key', axis=1)

        temp_df['distance'] = np.linalg.norm(np.array(temp_df['location_coord'].tolist()) - temp_df[['locv', 'loch']].values, axis=1)

        eligible_df = temp_df[temp_df['distance'] < radius].copy()
        eligible_df['rank'] = eligible_df.groupby('location_coord')['distance'].rank(method='first')

        final_assignment_df = eligible_df[
            eligible_df['rank'] == 1
        ].sort_values(['location_coord', 'rank'])

        pid_map = final_assignment_df.set_index('pid_y')['pid_x']
        new_pids = realmc.mm_dfs['mm_employee_master']['pid'].map(pid_map)
        realmc.mm_dfs['mm_employee_master']['pid'] = new_pids.fillna(realmc.mm_dfs['mm_employee_master']['pid']).astype(int)

        #adjust new employee's job 
        q1 = realmc.persondf['pid'].isin(final_assignment_df['pid_x'])
        realmc.persondf.loc[q1,'job'] = 3.0 #employed under MM

        #adjust old employee's job 
        q2 = realmc.persondf['pid'].isin(final_assignment_df['pid_y'])
        realmc.persondf.loc[q2,'job'] = 0.0 #no job

    def pay_employees(realmc):
        """
        This function pays the employees a wage of 40,000 / 12 * (1 + np.random.rand(len(realmc.mm_dfs['mm_employee_master']['pids']))/10) 
        """
        temp_employees = realmc.mm_dfs['mm_employee_master'].copy()
        pay = (1 + np.random.rand(len(temp_employees))/10) * 40000/12
        temp_employees['pay'] = pay #store amount paid

        #pay employees
        q1 = realmc.persondf['pid'].isin(temp_employees['pid'].tolist())
        realmc.persondf.loc[q1, 'savings'] += pay #add pay to savings

        #need to reflect expenses in location_master and books!
        temp_employees_groupby = temp_employees.groupby(['location_coord'])['pay'].sum().reset_index()
        #subtract from mm_location_master.balance
        merge_temp = pd.merge(realmc.mm_dfs['mm_location_master'], 
                              temp_employees_groupby, 
                              on='location_coord', 
                              how='left')
        merge_temp['pay'] = merge_temp['pay'].fillna(0) #handle location w/o employee expenses
        realmc.mm_dfs['mm_location_master']['balance'] = merge_temp['balance'] - merge_temp['pay']

        #subtract from latest mm_books.period_income
        q1 = realmc.mm_dfs['mm_books']['period_s'] == max(realmc.mm_dfs['mm_books']['period_s']) #get latest bank record
        merge_temp = pd.merge(realmc.mm_dfs['mm_books'].loc[q1], 
                              temp_employees_groupby, 
                              how='left', 
                              on='location_coord')
        realmc.mm_dfs['mm_books']['period_income'] -= merge_temp['pay'].fillna(0)

    radius = np.sqrt(2*(chunk_size/2)**2) #radius that employees must be within to stay employed
    replace = check_employees(realmc, radius)
    if len(replace) > 0: hire_employees(realmc, replace, radius)
    pay_employees(realmc)

def postopsMM(realmc, ccpdf: pd.DataFrame):
    """
    This function houses the processes for MM operations that run after the month is updated, after evolve.
    """
    mm_operations(realmc, ccpdf)
    if check_solvency(realmc):
        mm_hr(realmc)
        if check_solvency(realmc):
            mm_tax(realmc, ccpdf)
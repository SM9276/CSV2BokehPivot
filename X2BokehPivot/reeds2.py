'''
ReEDS 2.0 results metadata and preprocess functions.

When adding a new ReEDS result and associated presets, this should be the only file you need to modify.

There are three sections:
1. Preprocess functions: Only needed for a result if the gdx data needs to be manipulated
2. Columns metatdata: This allows column values from a result to be mapped to display categories, joined with other columns, and styled
3. Results metadata: This is where all result configuration happens
'''
from __future__ import division
import os
import pandas as pd
import numpy as np
import collections
import core
import copy
from pdb import set_trace as pdbst
from itertools import product
import ptvsd

##################   DEBUG CODE ##################
#In order to use place a breakpoint in a line of code and in Visual Studio Code
#and run in the run and debug mode (Ctrl+Shift+D)
# attach to VS Code debugger if this script was run with 
BOKEH_VS_DEBUG=True
# (place this just before the code you're interested in)
#if os.environ['BOKEH_VS_DEBUG'] == 'true':
#     # 5678 is the default attach port in the VS Code debug configurations
 #   print('Waiting for debugger attach')
 #   ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
  #  ptvsd.wait_for_attach()
###################################################

rb_globs = {'output_subdir':'/outputs/', 'test_file':'cap.csv', 'report_subdir':'/reeds2'}
this_dir_path = os.path.dirname(os.path.realpath(__file__))
df_deflator = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname(__file__), 'deflator.csv')), index_col=0)
coststreams = ['eq_gasaccounting_regional','eq_gasaccounting_national','eq_bioused','eq_gasused','eq_objfn_inv','eq_objfn_op']
vf_valstreams = ['eq_supply_demand_balance','eq_reserve_margin','eq_opres_requirement','eq_rec_requirement','eq_curt_gen_balance','eq_curtailment','eq_storage_in_max','eq_storage_in_min']
# valuestreams = ['eq_supply_demand_balance','eq_reserve_margin','eq_opres_requirement','eq_rec_requirement','eq_national_gen','eq_annual_cap','eq_curt_gen_balance','eq_curtailment','eq_storage_in_max','eq_storage_in_min','eq_emit_accounting','eq_mingen_lb','eq_mingen_ub','eq_rps_ofswind']
energy_valstreams = ['eq_supply_demand_balance','eq_curt_gen_balance','eq_curtailment','eq_storage_in_max','eq_storage_in_min']
cc_techs = ['hydro','wind-ons','wind-ofs','csp','upv','dupv','pumped-hydro','pumped-hydro-flex','battery', 'battery_2', 'battery_4', 'battery_6', 'battery_8', 'battery_10']
h2_techs = ['smr', 'smr-ccs', 'electrolyzer']
prod_techs = h2_techs + ['dac']
niche_techs =  ['hydro','csp','geothermal','beccs','lfill-gas','biopower']
price_types = ['load','res_marg','oper_res','state_rps','nat_gen']

#1. Preprocess functions for results_meta
def scale_column(df, **kw):
    df[kw['column']] = df[kw['column']] * kw['scale_factor']
    return df

def add_cooling_water(df, **kw):
    #load the tech mapping to ctt and wst
    df_tech_ctt_wst = pd.read_csv(this_dir_path + '/in/reeds2/tech_ctt_wst.csv')
    df_tech_ctt_wst['tech'] = df_tech_ctt_wst['tech'].str.lower()
    df = pd.merge(left=df, right=df_tech_ctt_wst, how='left', on=['tech'], sort=False)
    #fill na values
    df['wst'].fillna('other', inplace=True)
    df['ctt'].fillna('none', inplace=True)
    return df

def scale_column_filtered(df, **kw):
    cond = df[kw['by_column']].isin(kw['by_vals'])
    df.loc[cond, kw['change_column']] = df.loc[cond, kw['change_column']] * kw['scale_factor']
    return df

def sum_over_hours(df, **kw):
    # Determine group columns
    group_cols = kw['group_cols'][:]
    if 'year_col' in kw:
        group_cols.append(kw['year_col'])
    if 'month_col' in kw:
        group_cols.append(kw['month_col'])
    if 'day_col' in kw:
        group_cols.append(kw['day_col'])
    if 'hour_col' in kw:
        group_cols.append(kw['hour_col'])
    
    # Drop unnecessary columns
    if 'val_cols' in kw:
        drop_cols = [c for c in df.columns if c not in group_cols + kw['val_cols']]
    elif 'drop_cols' in kw:
        drop_cols = kw['drop_cols']
    else:
        drop_cols = []

    df = df.drop(drop_cols, axis='columns', errors='ignore')

    # Group by the specified columns and sum
    df = df.groupby(group_cols, sort=False, as_index=False).sum()

    return df

def sum_over_days(df, **kw):
    # Determine group columns
    group_cols = kw['group_cols'][:]
    if 'year_col' in kw:
        group_cols.append(kw['year_col'])
    if 'month_col' in kw:
        group_cols.append(kw['month_col'])
    if 'day_col' in kw:
        group_cols.append(kw['day_col'])
    
    # Drop unnecessary columns
    if 'val_cols' in kw:
        drop_cols = [c for c in df.columns if c not in group_cols + kw['val_cols']]
    elif 'drop_cols' in kw:
        drop_cols = kw['drop_cols']
    else:
        drop_cols = []

    df = df.drop(drop_cols, axis='columns', errors='ignore')

    # Group by the specified columns and sum
    df = df.groupby(group_cols, sort=False, as_index=False).sum()

    return df

def sum_over_months(df, **kw):
    # Handle month-based grouping
    if 'month_col' in kw:
        group_cols = kw['group_cols'] + [kw['month_col']]
    else:
        group_cols = kw['group_cols']

    if 'val_cols' in kw:
        drop_cols = [c for c in df.columns if c not in group_cols + kw['val_cols']]
    elif 'drop_cols' in kw:
        drop_cols = kw['drop_cols']
    else:
        drop_cols = []

    df = df.drop(drop_cols, axis='columns', errors='ignore')

    # Group by year, month (if provided), and any other specified columns
    df = df.groupby(group_cols, sort=False, as_index=False).sum()

    return df


def sum_over_cols(df, **kw):
    if 'val_cols' in kw:
        drop_cols = [c for c in df.columns if c not in kw['group_cols']+kw['val_cols']]
    elif 'drop_cols' in kw:
        drop_cols = kw['drop_cols']
    df = df.drop(drop_cols, axis='columns', errors='ignore')
    df =  df.groupby(kw['group_cols'], sort=False, as_index =False).sum()
    return df

def apply_inflation(df, **kw):
    df[kw['column']] = inflate_series(df[kw['column']])
    return df

def inflate_series(ser_in):
    return ser_in * 1/df_deflator.loc[int(core.GL['widgets']['var_dollar_year'].value), 'Deflator']

def gather_cost_types(df):
    #Gather lists of capital and operation labels
    cost_cats_df = df['cost_cat'].unique().tolist()
    df_cost_type = pd.read_csv(this_dir_path + '/in/reeds2/cost_cat_type.csv')
    #Make sure all cost categories in df are in df_cost_type and throw error if not!!
    if not set(cost_cats_df).issubset(df_cost_type['cost_cat'].values.tolist()):
        print('WARNING: Not all cost categories have been mapped!!! Categories without a mapping are:')
        print([ c for c in cost_cats_df if c not in df_cost_type['cost_cat'].values.tolist() ])
    cap_type_ls = [c for c in cost_cats_df if c in df_cost_type[df_cost_type['type']=='Capital']['cost_cat'].tolist()]
    op_type_ls = [c for c in cost_cats_df if c in df_cost_type[df_cost_type['type']=='Operation']['cost_cat'].tolist()]
    return cap_type_ls, op_type_ls

def pre_systemcost(dfs, **kw):
    df = dfs['sc'].copy()
    sw = dfs['sw'].set_index('switch')['value'].copy()
    scalars = dfs['scalars'].set_index('scalar')['value'].copy()
    sys_eval_years = int(sw['sys_eval_years'])
    trans_crp = int(scalars['trans_crp'])
    addyears = max(sys_eval_years, trans_crp)
    
    #Sometimes we want costs by BA and year, but otherwise aggregate costs nationwide by year:
    if 'maintain_ba_index' in kw and kw['maintain_ba_index'] == True:
        id_cols = ['r','year']
    else:
        id_cols = ['year']
    
    # Get historical years (for historical capex)
    firstmodelyear, lastmodelyear = int(df.year.min()), int(df.year.max())
    historical_years = range(firstmodelyear-sys_eval_years, firstmodelyear)

    #apply inflation and adjust to billion dollars
    df['Cost (Bil $)'] = inflate_series(df['Cost (Bil $)']) * 1e-9
    d = float(core.GL['widgets']['var_discount_rate'].value)
    y0 = int(core.GL['widgets']['var_pv_year'].value)

    cap_type_ls, op_type_ls = gather_cost_types(df)
    ### Get the list of transmission investment categories (subset of cap_type_ls)
    trans_cap_type_ls = [
        c for c in cap_type_ls if c in [
            'inv_converter_costs',
            'inv_substation_investment_costs',
            'inv_transmission_line_investment',
        ]
    ]
    nontrans_cap_type_ls = [c for c in cap_type_ls if c not in trans_cap_type_ls]

    #Calculate objective function system costs
    if 'objective' in kw and kw['objective'] == True:
        #Multiply all capital costs by pvf_capital and operation by pvf_onm
        df = pd.merge(left=df, right=dfs['pvf_cap'], how='left', on=['year'], sort=False)
        df = pd.merge(left=df, right=dfs['pvf_onm'], how='left', on=['year'], sort=False)
        cap_cond = df['cost_cat'].isin(cap_type_ls)
        onm_cond = df['cost_cat'].isin(op_type_ls)
        df.loc[cap_cond, 'Cost (Bil $)'] = df.loc[cap_cond, 'Cost (Bil $)'] * df.loc[cap_cond, 'pvfcap']
        df.loc[onm_cond, 'Cost (Bil $)'] = df.loc[onm_cond, 'Cost (Bil $)'] * df.loc[onm_cond, 'pvfonm']
        df.drop(['pvfcap','pvfonm'], axis='columns',inplace=True)
        #We don't add a discounted cost column
        return df

    #Annualize if specified
    if 'annualize' in kw and kw['annualize'] == True:
        #Turn each cost category into a column
        df = df.pivot_table(index=id_cols, columns='cost_cat', values='Cost (Bil $)').reset_index()
        if ('remove_existing' in kw) and (kw['remove_existing'] == True):
            df = df[df.year >= int(core.GL['widgets']['var_pv_year'].value)]
        #Add rows for all years (including extra years after end year for financial recovery)
        full_yrs = list(range(firstmodelyear - sys_eval_years, lastmodelyear + addyears + 1))

        if 'r' in df.columns:
            allyrs = pd.DataFrame(list(product(full_yrs,df.r.unique())), columns=['year','r'])
            df = pd.merge(allyrs,df,on=id_cols,how='left').set_index('year')
        else:
            df = df.set_index('year').reindex(full_yrs)

        ###### Add payments for pre-2010 capacity
        if ('remove_existing' not in kw) or (kw['remove_existing'] == False):
            ### Get modeled BAs
            rfeas = dfs['valid_ba_list'][0].values
            ### Get total historical capex in modeled BAs
            rsmap = dfs['rsmap'].set_index('rs')['*r']
            s2r = pd.concat([rsmap, pd.Series(index=rsmap.unique(), data=rsmap.unique())])
            s2r = s2r.loc[~s2r.index.duplicated()].copy()
            df_capex_init = dfs['df_capex_init']
            if 'maintain_ba_index' in kw and kw['maintain_ba_index'] == True:
                ### Keep data up until the year before the first modeled year
                historical_capex = df_capex_init.rename(
                    columns={'t':'year','region':'r'})
                historical_capex = historical_capex.loc[
                    df_capex_init.region.map(s2r).isin(rfeas)
                ].groupby(['year','r']).capex.sum().loc[:firstmodelyear-1,:] / 1e9
                ### Convert to output dollar year
                historical_capex = inflate_series(historical_capex)
                historical_capex = pd.DataFrame(historical_capex)
                ### Insert into full cost table
                df = df.set_index('r',append=True).join(historical_capex).reset_index('r')
                df.loc[:firstmodelyear-1,'inv_investment_capacity_costs'] = (
                    df.loc[:firstmodelyear-1,'capex'])
                df = df.drop('capex',axis=1)
            else:
                ### Keep data up until the year before the first modeled year
                historical_capex = df_capex_init.loc[
                    df_capex_init.region.map(s2r).isin(rfeas)
                ].groupby('t').capex.sum().loc[:firstmodelyear-1] / 1e9
                ### Convert to output dollar year
                historical_capex = inflate_series(historical_capex)
                ### Insert into full cost table
                df.loc[:firstmodelyear-1,'inv_investment_capacity_costs'] = (
                    historical_capex.loc[historical_years])

        #For capital costs, multiply by CRF to annualize, and sum over previous 20 years.
        #This requires 20 years before 2010 to sum properly.
        if 'crf_from_user' in kw and kw['crf_from_user'] == True:
            crf = pd.DataFrame({
                'year': full_yrs,
                'crf': d*(1+d)**sys_eval_years / ((1+d)**sys_eval_years - 1)
            }).set_index('year')
        #otherwise use the crf from model run
        else:
            crf = dfs['crf']
            crf = crf.set_index('year').reindex(full_yrs)
            crf = crf.interpolate(method='linear')
            crf['crf'] = crf['crf'].fillna(method='bfill')

        if 'shift_capital' in kw and kw['shift_capital'] == True:
            #This means we start capital payments in the year of the investment, even though loan payments
            #typically start in the following year, so that investments made in 2050 are still reflected in 2050 capital payments.
            #This requires dividing the crf by discount rate to result in the same present value calculation.
            crf = crf/(1+d)
        else:
            #This method reflects typical loan payments that start in the year after the loan.
            if 'maintain_ba_index' in kw and kw['maintain_ba_index'] == True:
                df[cap_type_ls] = df.groupby('r')[cap_type_ls].shift()
            else:
                df[cap_type_ls] = df[cap_type_ls].shift()
        df = pd.merge(left=df, right=crf, how='left',on=['year'], sort=False)
        df[cap_type_ls] = df[cap_type_ls].fillna(0)
        df[cap_type_ls] = df[cap_type_ls].multiply(df["crf"], axis="index")
        if 'r' in df.columns:
            df = df.reset_index()
            df[nontrans_cap_type_ls] = (df.groupby('r')[nontrans_cap_type_ls].rolling(sys_eval_years).sum().reset_index(drop=True))
            df[trans_cap_type_ls] = (df.groupby('r')[trans_cap_type_ls].rolling(trans_crp).sum().reset_index(drop=True))
        else:
            df[nontrans_cap_type_ls] = df[nontrans_cap_type_ls].rolling(sys_eval_years).sum()
            df[trans_cap_type_ls] = df[trans_cap_type_ls].rolling(trans_crp).sum()
            df = df.reset_index()
        # Remove years before first modeled year
        keep_yrs = list(range(firstmodelyear, lastmodelyear + addyears + 1))
        df = df.loc[df.year.isin(keep_yrs)]

        #For operation costs, simply fill missing years with model year values.
        # Assuming sys_eval_years = 20, operation payments last for 20 yrs starting in the modeled
        # year, so fill 19 empty years after the modeled year.
        if 'r' in df.columns:
            df.loc[:,op_type_ls] = df.groupby('r')[op_type_ls].fillna(method='ffill', limit=sys_eval_years-1)
        else:
            df.loc[:,op_type_ls] = df[op_type_ls].fillna(method='ffill', limit=sys_eval_years-1)
        df = df.fillna(0)
        df = pd.melt(df.reset_index(), id_vars=id_cols, value_vars=cap_type_ls + op_type_ls, var_name='cost_cat', value_name= 'Cost (Bil $)')
        
    #Add Dicounted Cost column (including for annualized)
    df['Discounted Cost (Bil $)'] = df['Cost (Bil $)'] / (1 + d)**(df['year'] - y0)
    return df

def pre_avgprice(dfs, **kw):
    #Depending on whether 'National' or 'BA'-level average cost if specified
    if kw['reg'] == 'National':
        df = pre_systemcost(dfs, annualize=True, shift_capital=kw['shift_capital'])
        #Read in load
        df_load = dfs['q']
        df_load_nat = df_load[df_load['type'] == 'load'].groupby('year')['q'].sum()
        df_load_nat = df_load_nat.to_frame()
        full_yrs = list(range(int(df['year'].min()), int(df['year'].max()) + 1))
        df_load_nat = df_load_nat.reindex(full_yrs)
        df_load_nat = df_load_nat.interpolate(method='ffill')

        df_natavgprice = pd.merge(left=df, right=df_load_nat, how='left',on=['year'], sort=False)
        df_natavgprice['Average cost ($/MWh)'] = df_natavgprice['Cost (Bil $)'] * 1e9 / df_natavgprice['q']

        return df_natavgprice

    elif kw['reg'] == 'BA':
        df = dfs['sc']
        #apply inflation and adjust to billion dollars
        df['Cost (Bil $)'] = inflate_series(df['Cost (Bil $)']) * 1e-9

        cap_type_ls, op_type_ls = gather_cost_types(df)

        df_rrs_map = pd.read_csv(this_dir_path + '/inputs/rsmap.csv').rename(columns={'*r':'r'})
        df_rrs_map.columns = ['regionnew','region']

        df_hours_map = pd.read_csv(this_dir_path + '/in/reeds2/m_map.csv')
        df_hours_width = pd.read_csv(this_dir_path + '/in/reeds2/m_bar_width.csv')
        df_hours_map = pd.merge(left=df_hours_map, right=df_hours_width, how='left',on=['display'],sort=False)
        df_hours_map.dropna(inplace=True)
        df_hours_map = df_hours_map.drop(columns = ['display'])
        df_hours_map.columns = ["timeslice","hours"]

        #-------Capital and operational costs--------
        #Aggregate costs to BA-level
        df = pd.merge(left=df, right=df_rrs_map, how='left',on=['region'], sort=False)
        df['regionnew'] = df['regionnew'].fillna(df['region'])
        df = df.groupby(['cost_cat','regionnew','year',])['Cost (Bil $)'].sum().reset_index()
        df.columns = ['cost_cat','region', 'year', 'Cost (Bil $)']

        region_ls = df['region'].unique().tolist()

        #Turn each cost category into a column
        df = df.pivot_table(index=['year'], columns=['region','cost_cat'], values='Cost (Bil $)')
        #Add rows for all years (including 20 years after end year) and fill
        full_yrs = list(range(df.index.min() - 19, df.index.max() + 21))
        df = df.reindex(full_yrs)
        #For capital costs, multiply by CRF to annualize, and sum over previous 20 years.
        #This requires 20 years before 2010 to sum properly, and we need to shift capital dataframe down
        #so that capital payments start in the year after the investment was made
        crf = dfs['crf']
        crf = crf.set_index('year').reindex(full_yrs)
        crf = crf.interpolate(method ='linear')
        crf['crf'] = crf['crf'].fillna(method='bfill')
        df = pd.merge(left=df, right=crf, how='left',on=['year'], sort=False)
        colname_ls = pd.MultiIndex.from_product([region_ls, cap_type_ls],names=['region', 'cost_cat'])
        colname_ls = [c for c in colname_ls if c in df.columns.tolist()]
        df[colname_ls] = df[colname_ls].shift().fillna(0)
        df[colname_ls] = df[colname_ls].multiply(df["crf"], axis="index")
        df[colname_ls] = df[colname_ls].rolling(20).sum()
        df = df.drop(columns = ['crf'])
        #Remove years before 2010
        full_yrs = list(range(df.index.min() + 19, df.index.max() + 1))
        df = df.reindex(full_yrs)

        #For operation costs, simply fill missing years with model year values.
        colname_ls = pd.MultiIndex.from_product([region_ls, op_type_ls],names=['region', 'cost_cat'])
        colname_ls = [c for c in colname_ls if c in df.columns.tolist()]
        df[colname_ls] = df[colname_ls].fillna(method='ffill')
        #The final year should only include capital payments because operation payments last for 20 yrs starting
        #in the model year, whereas capital payments last for 20 yrs starting in the year after the model year.
        df.loc[df.index.max(), colname_ls] = 0
        df = df.fillna(0)

        df = df.T
        df.index = pd.MultiIndex.from_tuples(df.index.values, names=('region','cost_cat'))
        df = df.reset_index()
        df = pd.melt(df, id_vars=['region','cost_cat'], value_vars=df.columns.tolist()[2:], var_name='year', value_name= 'Cost (Bil $)')
        df = df.pivot_table(index=['year','region'], columns=['cost_cat'], values='Cost (Bil $)')
        df = df.reset_index(level='region')

        #Add capacity payment for existing (pre-2010) generators (in billion $)
        df_existingpayment = dfs['existcap']
        df_existingpayment['existingcap'] = inflate_series(df_existingpayment['existingcap'])
        df = pd.merge(left=df, right=df_existingpayment, how='left',on=['year','region'], sort=False)
        df = df.fillna(0)
        df['inv_investment_capacity_costs'] = df['inv_investment_capacity_costs']+df['existingcap']
        df = df.drop(columns = 'existingcap')
        df = pd.melt(df.reset_index(), id_vars=['year','region'], value_vars=df.columns.tolist()[2:], var_name='cost_cat', value_name= 'Cost (Bil $)')

        #-------Capacity trading--------
        df_captrade = dfs['captrade']
        df_captrade = df_captrade.groupby(['rb_out', 'rb_in', 'season', 'year'])['Amount (MW)'].sum().reset_index()

        df_capprice = dfs['p']
        df_capprice = df_capprice.loc[df_capprice['type'] == 'res_marg'].reset_index()
        df_capprice = df_capprice[df_capprice.columns[3:]]
        df_capprice.columns = ['rb_out','season','year', 'p']

        df_captrade = pd.merge(left=df_captrade, right=df_capprice, how='left',on=['rb_out','season','year'], sort=False)
        df_captrade = df_captrade.dropna()
        df_captrade['cost_rb_out'] = df_captrade['p'] * df_captrade['Amount (MW)']  *1e3 / 1e9  #in billion dollars

        df_capimportcost = df_captrade.groupby(['rb_in','year',])['cost_rb_out'].sum().reset_index()
        df_capexportcost = df_captrade.groupby(['rb_out','year',])['cost_rb_out'].sum().reset_index()
        df_capexportcost['cost_rb_out'] = -df_capexportcost['cost_rb_out']

        df_capimportcost['cost_cat'] = 'cap_trade_import_cost'
        df_capimportcost.columns = ['region','year','Cost (Bil $)','cost_cat']
        df_capimportcost = df_capimportcost[['year','region','cost_cat','Cost (Bil $)']]
        df_capexportcost['cost_cat'] = 'cap_trade_export_cost'
        df_capexportcost.columns = ['region','year','Cost (Bil $)','cost_cat']
        df_capexportcost = df_capexportcost[['year','region','cost_cat','Cost (Bil $)']]
        df_captradecost = pd.concat([df_capimportcost,df_capexportcost], sort=False)

        #-------Energy trading--------
        df_gen = dfs['gen']
        df_gen = df_gen.groupby(['r', 'timeslice', 'year'])['Generation (GW)'].sum().reset_index()

        df_energyprice = dfs['p']
        df_energyprice = df_energyprice.loc[df_energyprice['type'] == 'load'].reset_index()
        df_energyprice = df_energyprice[df_energyprice.columns[3:]]
        df_energyprice.columns = ['r', 'timeslice', 'year', 'p']

        df_powfrac_up = dfs['powerfrac_upstream']
        df_powfrac_down = dfs['powerfrac_downstream']
        df_powfrac_up = df_powfrac_up.loc[df_powfrac_up['r'] != df_powfrac_up['rr'] ].reset_index()
        df_powfrac_down = df_powfrac_down.loc[df_powfrac_down['r'] != df_powfrac_down['rr'] ].reset_index()

        df_energyimport = pd.merge(left=df_powfrac_up, right=df_gen, how='left',on=['r','timeslice','year'], sort=False)
        df_energyimport = pd.merge(left=df_energyimport, right=df_energyprice, how='left',left_on=['rr','timeslice','year'], right_on=['r','timeslice','year'], sort=False)
        df_energyimport = df_energyimport.drop(columns = 'r_y')
        df_energyimport.columns = ['index', 'r', 'rr', 'timeslice', 'year', 'frac', 'Generation (GW)','p']
        df_energyimport = pd.merge(left=df_energyimport, right=df_hours_map, how='left',on=['timeslice'], sort=False)
        df_energyimport['importcost (Bil $)'] = df_energyimport['frac'] *df_energyimport['Generation (GW)'] * df_energyimport['p'] * df_energyimport['hours'] / 1e9
        df_energyimportcost = df_energyimport.groupby(['r', 'year'])['importcost (Bil $)'].sum().reset_index()

        df_energyexport = df_powfrac_down.groupby(['rr', 'timeslice', 'year'])['frac'].sum().reset_index()
        df_energyexport.columns = ['r', 'timeslice', 'year','frac']
        df_energyexport= pd.merge(left=df_energyexport, right=df_gen, how='left',on=['r','timeslice','year'], sort=False)
        df_energyexport = pd.merge(left=df_energyexport, right=df_energyprice, how='left',on=['r','timeslice','year'], sort=False)
        df_energyexport = pd.merge(left=df_energyexport, right=df_hours_map, how='left',on=['timeslice'], sort=False)

        df_energyexport['exportcost (Bil $)'] = df_energyexport['frac'] *df_energyexport['Generation (GW)'] * df_energyexport['p'] * df_energyexport['hours'] / 1e9
        df_energyexportcost = df_energyexport.groupby(['r', 'year'])['exportcost (Bil $)'].sum().reset_index()
        df_energyexportcost['exportcost (Bil $)'] = -df_energyexportcost['exportcost (Bil $)']

        df_energyimportcost['cost_cat'] = 'energy_trade_import_cost'
        df_energyimportcost.columns = ['region','year','Cost (Bil $)','cost_cat']
        df_energyimportcost = df_energyimportcost[['year','region','cost_cat','Cost (Bil $)']]
        df_energyexportcost['cost_cat'] = 'energy_trade_export_cost'
        df_energyexportcost.columns = ['region','year','Cost (Bil $)','cost_cat']
        df_energyexportcost = df_energyexportcost[['year','region','cost_cat','Cost (Bil $)']]
        df_energytradecost = pd.concat([df_energyimportcost,df_energyexportcost], sort=False)

        #-------Combine with load to calculate average cost--------
        df = pd.concat([df,df_captradecost,df_energytradecost], sort=False)

        df_load = dfs['q']
        df_load_ba = df_load[df_load['type'] == 'load'].groupby(['year','rb'])['q'].sum().reset_index()
        df_load_ba.columns = ['year','region','load (MWh)']

        df_baavgprice = pd.merge(left=df, right=df_load_ba, how='left',on=['year','region'], sort=False)
        df_baavgprice = df_baavgprice.dropna()
        df_baavgprice['Average cost ($/MWh)'] = df_baavgprice['Cost (Bil $)'] * 1e9 / df_baavgprice['load (MWh)']
        df_baavgprice.rename(columns={'region':'rb'}, inplace=True)

        return df_baavgprice

def pre_abatement_cost(dfs, **kw):
    if 'objective' in kw and kw['objective'] == True:
        #Preprocess costs
        df_sc = pre_systemcost(dfs, objective=True, shift_capital=kw['shift_capital'])
        df_sc['type'] = 'Cost (Bil $)'
        df_sc.rename(columns={'Cost (Bil $)':'val'}, inplace=True)
        #Preprocess emissions
        df_co2 = dfs['emit']
        df_co2.rename(columns={'CO2 (MMton)':'val'}, inplace=True)
        df_co2['val'] = df_co2['val'] * 1e-3 #converting to billion metric tons
        #Multiply by pvfonm to convert to total objective level of emissions for that year
        df_co2 = pd.merge(left=df_co2, right=dfs['pvf_onm'], how='left', on=['year'], sort=False)
        df_co2['val'] = df_co2['val'] * df_co2['pvfonm']
        df_co2.drop(['pvfonm'], axis='columns',inplace=True)
        #Add type and cost_cat columns so we can concatenate
        df_co2['type'] = 'CO2 (Bil metric ton)'
        df_co2['cost_cat'] = 'CO2 (Bil metric ton)'
        #Concatenate costs and emissions
        df = pd.concat([df_sc, df_co2],sort=False,ignore_index=True)

    elif 'annualize' in kw and kw['annualize'] == True:
        #Preprocess costs
        df_sc = pre_systemcost(dfs, annualize=True, crf_from_user=True, shift_capital=kw['shift_capital'])
        df_sc = sum_over_cols(df_sc, group_cols=['year','cost_cat'], drop_cols=['Discounted Cost (Bil $)'])
        df_sc['type'] = 'Cost (Bil $)'
        df_sc.rename(columns={'Cost (Bil $)':'val'}, inplace=True)
        #Preprocess emissions
        df_co2 = dfs['emit']
        df_co2.rename(columns={'CO2 (MMton)':'val'}, inplace=True)
        df_co2['val'] = df_co2['val'] * 1e-3 #converting to billion metric tons
        full_yrs = list(range(df_sc['year'].min(), df_sc['year'].max() + 1))
        df_co2 = df_co2.set_index('year').reindex(full_yrs).reset_index()
        df_co2['val'] = df_co2['val'].fillna(method='ffill')
        df_co2['type'] = 'CO2 (Bil metric ton)'
        df_co2['cost_cat'] = 'CO2 (Bil metric ton)'
        #Concatenate costs and emissions
        df = pd.concat([df_sc, df_co2],sort=False,ignore_index=True)
        #Add discounted value column
        d = float(core.GL['widgets']['var_discount_rate'].value)
        y0 = int(core.GL['widgets']['var_pv_year'].value)
        df['disc val'] = df['val'] / (1 + d)**(df['year'] - y0)
        #Add cumulative columns
        df['cum val'] = df.groupby('cost_cat')['val'].cumsum()
        df['cum disc val'] = df.groupby('cost_cat')['disc val'].cumsum()

    return df

def add_class(df, **kw):
    cond = df['tech'].str.contains('_', regex=False)
    #Assume class is at the end, after the final underscore:
    df.loc[cond, 'class']='class_' + df.loc[cond, 'tech'].str.split('_').str[-1]
    return df

def map_rs_to_rb(df, **kw):
    df_hier = pd.read_csv(this_dir_path + '/inputs/rsmap.csv').rename(columns={'*r':'r'})
    dict_hier = dict(zip(df_hier['rs'], df_hier['r']))
    df.loc[df['region'].isin(dict_hier.keys()), 'region'] = df['region'].map(dict_hier)
    df.rename(columns={'region':'rb'}, inplace=True)
    if 'groupsum' in kw:
        df = df.groupby(kw['groupsum'], sort=False, as_index=False).sum()
    return df

def sort_timeslices(df, **kw):
    ### Use either h or timeslice as column name
    timeslice = 'timeslice' if 'timeslice' in df.columns else 'h'
    timeslices = df[timeslice].unique()
    ### If using h17, apply custom labels
    if len(timeslices) <= 17:
        replace = {
            'h1': 'h01 (Summer 10pm-6am)',
            'h2': 'h02 (Summer 6am-1pm)',
            'h3': 'h03 (Summer 1pm-5pm)',
            'h4': 'h04 (Summer 5pm-10pm)',
            'h5': 'h05 (Fall 10pm-6am)',
            'h6': 'h06 (Fall 6am-1pm)',
            'h7': 'h07 (Fall 1pm-5pm)',
            'h8': 'h08 (Fall 5pm-10pm)',
            'h9': 'h09 (Winter 10pm-6am)',
            'h10': 'h10 (Winter 6am-1pm)',
            'h11': 'h11 (Winter 1pm-5pm)',
            'h12': 'h12 (Winter 5pm-10pm)',
            'h13': 'h13 (Spring 10pm-6am)',
            'h14': 'h14 (Spring 6am-1pm)',
            'h15': 'h15 (Spring 1pm-5pm)',
            'h16': 'h16 (Spring 5pm-10pm)',
            'h17': 'h17 (Summer Peak)',
        }
        df = df.replace({timeslice:replace}).copy()
        return df
    ###### Otherwise, remove the h prefix, then sort by the timeslice column
    ### Replace the h with left-filled zeros
    def remove_h(x):
        try:
            return '{:0>6}'.format(int(x[1:]))
        except ValueError:
            return x
    df[timeslice] = df[timeslice].map(remove_h)
    ### Sort it
    df = df.sort_values(timeslice, ascending=True)
    return df

def remove_ba(df, **kw):
    df = df[~df['region'].astype(str).str.startswith('p')].copy()
    df.rename(columns={'region':'rs'}, inplace=True)
    return df

def pre_gen_w_load(dfs, **kw):
    #Aggregate results to national
    dfs['gen'] = sum_over_cols(dfs['gen'], drop_cols=['rb','vintage'], group_cols=['tech', 'year'])
    dfs['gen_uncurt'] = sum_over_cols(dfs['gen_uncurt'], drop_cols=['rb','vintage'], group_cols=['tech', 'year'])
    #Outer join generation, fill any missing gen with 0, and then fill missing gen_uncurt with gen
    df = pd.merge(left=dfs['gen'], right=dfs['gen_uncurt'], how='outer', on=['tech','year'], sort=False)
    df['Gen (TWh)'].fillna(0, inplace=True)
    df['Gen Uncurt (TWh)'].fillna(df['Gen (TWh)'], inplace=True)
    #Scale generation to TWh
    df[['Gen (TWh)','Gen Uncurt (TWh)']] = df[['Gen (TWh)','Gen Uncurt (TWh)']] * 1e-6
    #Scale Load to TWh
    dfs['load']['TWh'] = dfs['load']['TWh'] * 1e-6
    #pivot load out to include losses columns
    dfs['load'] = dfs['load'].pivot_table(index=['year'], columns='type', values='TWh').reset_index()
    #Join Load
    df = pd.merge(left=df, right=dfs['load'], how='left', on=['year'], sort=False)
    #Calculate Gen Fracs
    df['Gen Frac of Load'] = df['Gen (TWh)'] / df['load']
    df['Gen Frac of Load Plus Stor/Trans Loss'] = df['Gen (TWh)'] / (df['load'] + df['storage'] + df['trans'])
    df['Gen Uncurt Frac of Load'] = df['Gen Uncurt (TWh)'] / df['load']
    df['Gen Uncurt Frac of Load Plus All Losses'] = df['Gen Uncurt (TWh)'] / (df['load'] + df['storage'] + df['trans'] + df['curt'])
    return df

def pre_val_streams(dfs, **kw):
    index_cols = ['tech', 'vintage', 'rb', 'year']
    inv_vars = ['inv','inv_refurb','upgrades','invtran']
    cap_vars = ['cap','captran']
    cum_vars = ['gen','cap','opres','storage_in','captran','flow','opres_flow','prmtrade','storage_in_pvb_p','storage_in_pvb_g','cap_sdbin','storage_level','recs','gen_pvb_p','gen_pvb_b','produce']

    if 'remove_inv' in kw:
        dfs['vs'] = dfs['vs'][~dfs['vs']['var_name'].isin(inv_vars)].copy()

    if 'uncurt' in kw:
        #For techs that are in gen_uncurt, use gen_uncurt instead of gen
        dfs['gen'] = dfs['gen'][~dfs['gen']['tech'].isin(dfs['gen_uncurt']['tech'].unique())].copy()
        dfs['gen'] = pd.concat([dfs['gen'], dfs['gen_uncurt']],sort=False,ignore_index=True)

    if 'investment_only' in kw:
        #Analyze only investment years
        #The first attempt of this was to use the ratio of new vs cumulative capacity in a vintage, but this has issues
        #because of the mismatch in regionality between capacity and generation, meaning ba-level capacity ratio may not
        #even out value streams.
        #First, use the capacity/investment linking equations with the investment and capacity variables to find the
        #scaling factors between investment and capacity value streams
        linking_eqs = ['eq_cap_new_noret','eq_cap_new_retub','eq_cap_new_retmo','eq_cap_upgrade','eq_captran'] #eq_cap_new_retmo also includes previous year's CAP, is this bad?!
        df_vs_links = dfs['vs'][dfs['vs']['con_name'].isin(linking_eqs)].copy()
        df_vs_inv = df_vs_links[df_vs_links['var_name'].isin(inv_vars)].copy()
        df_vs_cap = df_vs_links[df_vs_links['var_name'].isin(cap_vars)].copy()
        df_vs_inv = sum_over_cols(df_vs_inv, group_cols=index_cols, drop_cols=['var_name','con_name'])
        df_vs_cap = sum_over_cols(df_vs_cap, group_cols=index_cols, drop_cols=['var_name','con_name'])
        #merge inner with df_vs_inv so that we're only looking at cumulative value streams in investment years.
        df_scale = pd.merge(left=df_vs_inv, right=df_vs_cap, how='inner', on=index_cols, sort=False)
        df_scale['mult'] = df_scale['$_x'] / df_scale['$_y'] * -1
        df_scale = df_scale[index_cols + ['mult']]
        #Gather cumulative value streams. NEED TO ADD TRANSMISSION
        df_cum = dfs['vs'][dfs['vs']['var_name'].isin(cum_vars)].copy()
        #Left merge with df_scale to keep only the cumulative streams in investment years
        df_cum = pd.merge(left=df_scale, right=df_cum, how='left', on=index_cols, sort=False)
        #Scale the cumulative value streams
        df_cum['$'] = df_cum['$'] * df_cum['mult']
        df_cum.drop(['mult'], axis='columns',inplace=True)
        #Concatenate modified cumulative value streams with the rest of the value streams
        dfs['vs'] = dfs['vs'][dfs['vs']['var_name'].isin(inv_vars)].copy()
        dfs['vs'] = pd.concat([dfs['vs'], df_cum],sort=False,ignore_index=True)
        #Adjust generation based on the same scaling factor. Not sure this is exactly right, but if
        #value streams for GEN have scaled, it makes sense to attribute this to quantity of energy being scaled,
        #rather than prices changing.
        dfs['gen'] = pd.merge(left=df_scale, right=dfs['gen'], how='left', on=index_cols, sort=False)
        dfs['gen']['MWh'] = dfs['gen']['MWh'] * dfs['gen']['mult']
        dfs['gen'].drop(['mult'], axis='columns',inplace=True)

    #apply inflation
    dfs['vs']['$'] = inflate_series(dfs['vs']['$'])

    if 'value_factors' in kw:
        #sum over vintage in both generation and value streams:
        df_gen = sum_over_cols(dfs['gen'], group_cols=['tech','rb','year'], val_cols=['MWh'])
        df = sum_over_cols(dfs['vs'], group_cols=['tech','rb','year','con_name'], val_cols=['$'])
        #Reduce value streams to only the ones we care about
        df = df[df['con_name'].isin(vf_valstreams)].copy()
        #Include all con_name options for each tech,rb,year combo and fill na with zero
        df = df.pivot_table(index=['tech','rb','year'], columns='con_name', values='$').reset_index()
        df = pd.melt(df, id_vars=['tech','rb','year'], var_name='con_name', value_name= '$')
        df['$'].fillna(0, inplace=True)
        #convert value streams from bulk $ as of discount year to annual as of model year
        df = pd.merge(left=df, right=dfs['pvf_onm'], how='left', on=['year'], sort=False)
        df['$'] = df['$'] / dfs['cost_scale'].iloc[0,0] / df['pvfonm']
        df.drop(['pvfonm'], axis='columns',inplace=True)

        #get requirement prices and quantities and build benchmark value streams
        dfs['p']['p'] = inflate_series(dfs['p']['p'])
        df_bm = pd.merge(left=dfs['q'], right=dfs['p'], how='left', on=['type', 'subtype', 'rb', 'timeslice', 'year'], sort=False)
        df_bm['p'].fillna(0, inplace=True)
        #Add con_name:
        types = ['load','res_marg','oper_res','state_rps','curt_realize','curt_cause'] #the curt ones don't exist, they are just placeholders for the mapping.
        df_bm = df_bm[df_bm['type'].isin(types)].copy()
        df_con_type = pd.DataFrame({'type':types,'con_name':vf_valstreams})
        df_bm = pd.merge(left=df_bm, right=df_con_type, how='left', on=['type'], sort=False)
        #drop type and subtype because we're using con_name from here on
        df_bm.drop(['type','subtype'], axis='columns', inplace=True)
        #columns of df_bm at this point are rb,timeslice,year,con_name,p,q

        #Calculate annual load for use in benchmarks
        df_load = dfs['q'][dfs['q']['type']=='load'].copy()
        df_load = sum_over_cols(df_load, group_cols=['rb', 'year'], val_cols=['q'])

        #All-in benchmarks. These assume the benchmark tech provides all value streams at requirement levels,
        #and prices are calculated by dividing value by load.
        df_bm_allin = df_bm.copy()
        df_bm_allin['$'] = df_bm_allin['p'] * df_bm_allin['q']
        df_bm_allin = sum_over_cols(df_bm_allin, group_cols=['con_name', 'rb', 'year'], val_cols=['$'])
        df_bm_allin = pd.merge(left=df_bm_allin, right=df_load, how='left', on=['rb', 'year'], sort=False)

        #local all-in (weighted) benchmark. First calculate prices as $ of requirement divided by MWh energy
        df_bm_allin_loc = df_bm_allin.copy()
        df_bm_allin_loc['$ all-in loc'] = df_bm_allin_loc['$'] / df_bm_allin_loc['q']
        df_bm_allin_loc.drop(['$','q'], axis='columns', inplace=True)
        df = pd.merge(left=df, right=df_bm_allin_loc, how='left', on=['year','rb','con_name'], sort=False)

        #system-wide all-in (weighted) benchmark
        df_bm_allin_sys = sum_over_cols(df_bm_allin, group_cols=['year','con_name'], val_cols=['$','q'])
        df_bm_allin_sys['$ all-in sys'] = df_bm_allin_sys['$'] / df_bm_allin_sys['q']
        df_bm_allin_sys.drop(['$','q'], axis='columns', inplace=True)
        df = pd.merge(left=df, right=df_bm_allin_sys, how='left', on=['year','con_name'], sort=False)

        #Flat-block benchmarks. These assume the tech provides energy + reserve margin only.
        flatblock_cons = ['eq_supply_demand_balance','eq_reserve_margin']
        df_bm_flat_loc = df_bm[df_bm['con_name'].isin(flatblock_cons)].copy()
        #Add hours
        df_hours = pd.read_csv(this_dir_path + '/in/reeds2/hours.csv')
        df_bm_flat_loc = pd.merge(left=df_bm_flat_loc, right=df_hours, how='left', on=['timeslice'], sort=False)
        #First we calculate p as annual $ for 1 kW flat-block tech,
        #so we need to multiply eq_supply_demand_balance price ($/MWh) by hours divided by 1000 (eq_reserve_margin price is fine as is) and sum across timeslices
        load_cond = df_bm_flat_loc['con_name'] == 'eq_supply_demand_balance'
        df_bm_flat_loc.loc[load_cond, 'p'] = df_bm_flat_loc.loc[load_cond, 'p'] * df_bm_flat_loc.loc[load_cond, 'hours'] / 1000
        df_bm_flat_loc = sum_over_cols(df_bm_flat_loc, group_cols=['year','rb','con_name'], val_cols=['p'])
        #Convert to $/MWh from $/kW: 1 kW flat block produces 8.76 MWh annual energy.
        df_bm_flat_loc['$ flat loc'] = df_bm_flat_loc['p'] / 8.76
        df_bm_flat_loc.drop(['p'], axis='columns', inplace=True)
        df = pd.merge(left=df, right=df_bm_flat_loc, how='left', on=['year','rb','con_name'], sort=False)

        #system flat benchmark. We weight the local prices by annual load
        df_bm_flat_sys = pd.merge(left=df_bm_flat_loc, right=df_load, how='left', on=['rb', 'year'], sort=False)
        df_bm_flat_sys['$'] = df_bm_flat_sys['$ flat loc'] * df_bm_flat_sys['q']
        df_bm_flat_sys = sum_over_cols(df_bm_flat_sys, group_cols=['con_name', 'year'], val_cols=['$','q'])
        df_bm_flat_sys['$ flat sys'] = df_bm_flat_sys['$'] / df_bm_flat_sys['q']
        df_bm_flat_sys.drop(['$','q'], axis='columns', inplace=True)
        df = pd.merge(left=df, right=df_bm_flat_sys, how='left', on=['year','con_name'], sort=False)

        #Merge with generation so we can calculate $ associated with all benchmarks
        df = pd.merge(left=df, right=df_gen, how='left', on=['tech','rb','year'], sort=False)
        #Now convert all prices to values
        vf_cols = ['$ all-in loc','$ all-in sys','$ flat loc','$ flat sys']
        df[vf_cols] = df[vf_cols].multiply(df['MWh'], axis="index")
        df.drop(['MWh'], axis='columns',inplace=True)
        df_gen['con_name'] = 'MWh'
        df_gen.rename(columns={'MWh': '$'}, inplace=True) #So we can concatenate
        for vf_col in vf_cols:
            df_gen[vf_col] = df_gen['$'] #Duplicating MWh across all the value columns
        df = pd.concat([df, df_gen],sort=False,ignore_index=True)
        df = df.fillna(0)
        return df

    #Use pvf_capital to convert to present value as of data year (model year for CAP and GEN but investment year for INV,
    #although i suppose certain equations, e.g. eq_cap_new_retmo also include previous year's CAP).
    df = pd.merge(left=dfs['vs'], right=dfs['pvf_cap'], how='left', on=['year'], sort=False)
    df['Bulk $'] = df['$'] / dfs['cost_scale'].iloc[0,0] / df['pvfcap']
    df.drop(['pvfcap', '$'], axis='columns',inplace=True)

    #Add total value and total cost
    # df_val = df[df['con_name'].isin(valuestreams)].copy()
    # df_val = sum_over_cols(df_val, group_cols=index_cols, drop_cols=['var_name','con_name'])
    # df_val['var_name'] = 'val_tot'
    # df_val['con_name'] = 'val_tot'
    # df_cost = df[df['con_name'].isin(coststreams)].copy()
    # df_cost = sum_over_cols(df_cost, group_cols=index_cols, drop_cols=['var_name','con_name'])
    # df_cost['var_name'] = 'cost_tot'
    # df_cost['con_name'] = 'cost_tot'
    # df = pd.concat([df, df_val, df_cost],sort=False,ignore_index=True)

    #Preprocess gen: convert from annual MWh to bulk MWh present value as of data year
    df_gen = dfs['gen'].groupby(index_cols, sort=False, as_index =False).sum()
    df_gen = pd.merge(left=df_gen, right=dfs['pvf_cap'], how='left', on=['year'], sort=False)
    df_gen = pd.merge(left=df_gen, right=dfs['pvf_onm'], how='left', on=['year'], sort=False)
    df_gen['MWh'] = df_gen['MWh'] * df_gen['pvfonm'] / df_gen['pvfcap'] #This converts to bulk MWh present value as of data year
    df_gen.rename(columns={'MWh': 'Bulk $'}, inplace=True) #So we can concatenate
    df_gen['var_name'] = 'MWh'
    df_gen['con_name'] = 'MWh'
    df_gen.drop(['pvfcap', 'pvfonm'], axis='columns',inplace=True)
    df = pd.concat([df, df_gen],sort=False,ignore_index=True)

    #Preprocess capacity: map i to n, convert from MW to kW, reformat columns to concatenate
    df_cap = map_rs_to_rb(dfs['cap'])
    df_cap =  df_cap.groupby(index_cols, sort=False, as_index =False).sum()
    df_cap['MW'] = df_cap['MW'] * 1000 #Converting to kW
    df_cap.rename(columns={'MW': 'Bulk $'}, inplace=True) #So we can concatenate
    df_cap['var_name'] = 'kW'
    df_cap['con_name'] = 'kW'
    df = pd.concat([df, df_cap],sort=False,ignore_index=True)

    #Add discounted $ using interface year
    d = float(core.GL['widgets']['var_discount_rate'].value)
    y0 = int(core.GL['widgets']['var_pv_year'].value)
    df['Bulk $ Dis'] = df['Bulk $'] / (1 + d)**(df['year'] - y0) #This discounts $, MWh, and kW, important for NVOE, NVOC, LCOE, etc.

    #Add new columns
    df['tech, vintage'] = df['tech'] + ', ' + df['vintage']
    df['var, con'] = df['var_name'] + ', ' + df['con_name']
    #Make adjusted con_name column where all objective costs are replaced with var_name, con_name
    df['con_adj'] = df['con_name']
    df.loc[df['con_name'].isin(['eq_objfn_inv','eq_objfn_op']), 'con_adj'] = df.loc[df['con_name'].isin(['eq_objfn_inv','eq_objfn_op']), 'var, con']

    if 'competitiveness' in kw:
        df = df[index_cols+['Bulk $ Dis','con_name']]

        #Total cost for each tech
        df_cost = df[df['con_name'].isin(coststreams)].copy()
        df_cost.rename(columns={'Bulk $ Dis':'Cost $'}, inplace=True)
        df_cost = sum_over_cols(df_cost, group_cols=index_cols, drop_cols=['con_name'])
        df_cost['Cost $'] = df_cost['Cost $'] * -1

        #Total Value for each tech
        df_value = df[df['con_name'].isin(vf_valstreams)].copy()
        df_value.rename(columns={'Bulk $ Dis':'Value $'}, inplace=True)
        df_value = sum_over_cols(df_value, group_cols=index_cols, drop_cols=['con_name'])

        #Total energy for each tech
        df_energy = df[df['con_name'] == 'MWh'].copy()
        df_energy.rename(columns={'Bulk $ Dis':'MWh'}, inplace=True)
        df_energy = sum_over_cols(df_energy, group_cols=index_cols, drop_cols=['con_name'])

        #Benchmark price, excluding national_generation constraint, with the assumption that this constraint
        #represents a simple forcing function rather than a policy. If the constraint represents a CES or RPS policy e.g.,
        #we probably should use 'tot' rather than the sum of 'load','res_marg','oper_res', and 'state_rps'.
        df_benchmark = pre_prices(dfs)
        df_benchmark = df_benchmark[df_benchmark['type'].isin(['q_load','load','res_marg','oper_res','state_rps'])].copy()
        df_benchmark = sum_over_cols(df_benchmark, group_cols=['type','year'], val_cols=['$'])
        df_benchmark = df_benchmark.pivot_table(index=['year'], columns=['type'], values='$').reset_index()
        df_benchmark['P_b'] = (df_benchmark['load'] + df_benchmark['res_marg'] + df_benchmark['oper_res'] + df_benchmark['state_rps']) / df_benchmark['q_load']
        df_benchmark = df_benchmark[['year','P_b']].copy()

        #Merge all the dataframes
        df = pd.merge(left=df_cost, right=df_energy, how='left', on=index_cols, sort=False)
        df = pd.merge(left=df, right=df_value, how='left', on=index_cols, sort=False)
        df = pd.merge(left=df, right=df_benchmark, how='left', on=['year'], sort=False)

        #Build metrics
        df['LCOE'] = df['Cost $'] / df['MWh']
        df['LVOE'] = df['Value $'] / df['MWh']
        df['NVOE'] = df['LVOE'] - df['LCOE']
        df['BCR'] = df['Value $'] / df['Cost $']
        df['PLCOE'] = df['P_b'] * df['Cost $'] / df['Value $']
        df['value factor'] = df['LVOE'] / df['P_b']
        df['integration cost'] = df['P_b'] - df['LVOE']
        df['SLCOE'] = df['LCOE'] + df['integration cost']
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df

def pre_reduced_cost(df, **kw):
    df['irbv'] = df['tech'] + ' | ' + df['region'] + ' | ' + df['bin'] + ' | ' + df['variable']
    return df

def pre_lcoe(dfs, **kw):
    #Apply inflation
    dfs['lcoe']['$/MWh'] = inflate_series(dfs['lcoe']['$/MWh'])
    #Merge with available capacity
    df = pd.merge(left=dfs['lcoe'], right=dfs['avail'], how='left', on=['tech', 'region', 'year', 'bin'], sort=False)
    df['available MW'].fillna(0, inplace=True)
    df['available'] = 'no'
    df.loc[df['available MW'] > 0.001, 'available'] = 'yes'
    #Merge with chosen capacity
    df = pd.merge(left=df, right=dfs['inv'], how='left', on=['tech', 'vintage', 'region', 'year', 'bin'], sort=False)
    df['chosen MW'].fillna(0, inplace=True)
    df['chosen'] = 'no'
    df.loc[df['chosen MW'] != 0, 'chosen'] = 'yes'
    #Add icrb column
    df['icrb'] = df['tech'] + ' | ' + df['vintage'] + ' | ' + df['region'] + ' | ' + df['bin']
    return df

def pre_curt_new(dfs, **kw):
    df = pd.merge(left=dfs['gen_uncurt'], right=dfs['curt_rate'], how='left',on=['tech', 'region', 'timeslice', 'year'], sort=False)
    df['Curt Rate']=df['Curt Rate'].fillna(0)
    if 'annual' in kw:
        df['MWh curt'] = df['Curt Rate'] * df['MWh uncurt']
        df = sum_over_cols(df, group_cols=['tech', 'region', 'year'], drop_cols=['timeslice','Curt Rate'])
        df['Curt Rate'] = df['MWh curt'] / df['MWh uncurt']
        df['Curt Rate']=df['Curt Rate'].fillna(0)
    return df

def pre_cc_new(dfs, **kw):
    df = pd.merge(left=dfs['cap'], right=dfs['cc'], how='left',on=['tech', 'region', 'season', 'year'], sort=False)
    df['CC Rate']=df['CC Rate'].fillna(0)
    return df

def pre_firm_cap(dfs, **kw):
    #aggregate capacity to ba-level
    df_cap = map_rs_to_rb(dfs['cap'], groupsum=['tech','rb','year'])
    #Add seasons to capacity dataframe
    dftemp = pd.DataFrame({'season':dfs['firmcap']['season'].unique().tolist()})
    dftemp['temp'] = 1
    df_cap['temp'] = 1
    df_cap = pd.merge(left=df_cap, right=dftemp, how='left',on=['temp'], sort=False)
    df_cap.drop(columns=['temp'],inplace=True)
    df = pd.merge(left=df_cap, right=dfs['firmcap'], how='left',on=['tech', 'rb', 'year','season'], sort=False)
    df = df.fillna(0)
    df[['Capacity (GW)','Firm Capacity (GW)']] = df[['Capacity (GW)','Firm Capacity (GW)']] * 1e-3
    if 'ba' in kw and kw['ba'] == True:
        df['Capacity Credit'] = df['Firm Capacity (GW)'] / df['Capacity (GW)']
    return df

def pre_curt(dfs, **kw):
    df = pd.merge(left=dfs['gen_uncurt'], right=dfs['gen'], how='left',on=['tech', 'vintage', 'rb', 'year'], sort=False)
    df['MWh']=df['MWh'].fillna(0)
    df['Curt Rate'] = 1 - df['MWh']/df['MWh uncurt']
    df_re_n = sum_over_cols(dfs['gen_uncurt'], group_cols=['rb','year'], drop_cols=['tech','vintage'])
    df_re_nat = sum_over_cols(dfs['gen_uncurt'], group_cols=['year'], drop_cols=['tech','vintage','rb'])
    df_load_nat = sum_over_cols(dfs['load'], group_cols=['year'], drop_cols=['rb'])
    df_vrepen_n = pd.merge(left=dfs['load'], right=df_re_n, how='left',on=['rb', 'year'], sort=False)
    df_vrepen_n['VRE penetration n'] = df_vrepen_n['MWh uncurt'] / df_vrepen_n['MWh load']
    df_vrepen_n = df_vrepen_n[['rb','year','VRE penetration n']]
    df_vrepen_nat = pd.merge(left=df_load_nat, right=df_re_nat, how='left',on=['year'], sort=False)
    df_vrepen_nat['VRE penetration nat'] = df_vrepen_nat['MWh uncurt'] / df_vrepen_nat['MWh load']
    df_vrepen_nat = df_vrepen_nat[['year','VRE penetration nat']]
    df = pd.merge(left=df, right=df_vrepen_n, how='left',on=['rb', 'year'], sort=False)
    df = pd.merge(left=df, right=df_vrepen_nat, how='left',on=['year'], sort=False)
    return df

def pre_curt_iter(dfs, **kw):
    df_gen = dfs['gen_uncurt']
    df_curt = dfs['curt']
    df_gen = df_gen[df_gen['tech'].isin(df_curt['tech'].unique())].copy()
    df_gen['type'] = 'gen'
    df_curt['type'] = 'curt'
    df = pd.concat([df_gen, df_curt],sort=False,ignore_index=True)
    return df

def pre_cc_iter(dfs, **kw):
    df_cap = dfs['cap']
    df_cap_firm = dfs['cap_firm']
    df_cap = df_cap[df_cap['tech'].isin(df_cap_firm['tech'].unique())].copy()
    df_cap['type'] = 'cap'
    df_cap_firm['type'] = 'cap_firm'
    seasons = list(df_cap_firm['season'].unique())
    df_season = pd.DataFrame({'season':seasons,'temp':[1]*len(seasons)})
    df_cap['temp'] = 1
    df_cap = pd.merge(left=df_cap, right=df_season, how='left',on=['temp'], sort=False)
    df_cap.drop(['temp'], axis='columns',inplace=True)
    df = pd.concat([df_cap, df_cap_firm],sort=False,ignore_index=True)
    return df

def pre_cf(dfs, **kw):
    index_cols = ['tech', 'vintage', 'rb', 'year']
    dfs['cap'] = map_rs_to_rb(dfs['cap'])
    dfs['cap'] =  dfs['cap'].groupby(index_cols, sort=False, as_index=False).sum()
    df = pd.merge(left=dfs['cap'], right=dfs['gen'], how='left',on=index_cols, sort=False)
    df['MWh']=df['MWh'].fillna(0)
    df['CF'] = df['MWh']/(df['MW']*8760)
    return df

def pre_h2_cf(dfs, **kw):
    index_cols = ['tech', 'rb', 'year']
    dfs['cap'] = map_rs_to_rb(dfs['cap'])
    dfs['cap'] =  dfs['cap'].groupby(index_cols, sort=False, as_index=False).sum()
    df = pd.merge(left=dfs['cap'], right=dfs['prod'], how='left',on=index_cols, sort=False)
    df['Production (tonnes)']=df['Production (tonnes)'].fillna(0)
    df['CF'] = df['Production (tonnes)']/(df['Capacity (tonnes)']*8760)
    return df

def pre_new_vre_cf(dfs, **kw):
    index_cols = ['tech', 'rs', 'year']
    dfs['gen_new_uncurt'] = sum_over_cols(dfs['gen_new_uncurt'], group_cols=index_cols, val_cols=['MWh'])
    df = pd.merge(left=dfs['gen_new_uncurt'], right=dfs['cap_new'], how='inner',on=index_cols, sort=False)
    df['CF'] = df['MWh']/(df['MW']*8760)
    return df

def pre_vre_vs_stor(dfs, **kw):
    df_gen = dfs['gen'].copy()
    df_stor = dfs['stor'].copy()
    df_stor = df_stor[df_stor['type'] == 'in'].copy()
    df_gen = sum_over_cols(df_gen, group_cols=['technology', 'year'], val_cols=['Generation (TWh)'])
    df_stor = sum_over_cols(df_stor, group_cols=['technology', 'year'], val_cols=['Storage (TWh)'])
    df_gen['Generation (TWh)'] = df_gen['Generation (TWh)']/1e6
    df_stor['Storage (TWh)'] = df_stor['Storage (TWh)']/1e6
    df_wind = df_gen[df_gen['technology'].str.startswith('wind')].copy()
    df_pv = df_gen[df_gen['technology'].str.contains('upv|distpv')].copy()
    df_bat = df_stor[df_stor['technology'].str.startswith('battery')].copy()
    df_wind = sum_over_cols(df_wind, group_cols=['year'], val_cols=['Generation (TWh)'])
    df_pv = sum_over_cols(df_pv, group_cols=['year'], val_cols=['Generation (TWh)'])
    df_bat = sum_over_cols(df_bat, group_cols=['year'], val_cols=['Storage (TWh)'])
    df_storall = sum_over_cols(df_stor, group_cols=['year'], val_cols=['Storage (TWh)'])
    df_wind = df_wind.rename(columns={'Generation (TWh)': 'Wind (TWh)'}).set_index('year')
    df_pv = df_pv.rename(columns={'Generation (TWh)': 'PV (TWh)'}).set_index('year')
    df_bat = df_bat.rename(columns={'Storage (TWh)': 'Battery (TWh)'}).set_index('year')
    df_storall = df_storall.set_index('year')
    df = pd.concat([df_wind, df_pv, df_storall, df_bat], axis=1).fillna(0).reset_index().sort_values('year')
    df['VRE (TWh)'] = df['Wind (TWh)'] + df['PV (TWh)']
    return df

def pre_prices(dfs, **kw):
    #Apply inflation
    dfs['p']['p'] = inflate_series(dfs['p']['p'])
    #Join prices and quantities
    df = pd.merge(left=dfs['q'], right=dfs['p'], how='left', on=['type', 'subtype', 'rb', 'timeslice', 'year'], sort=False)
    df['p'].fillna(0, inplace=True)
    #Calculate $
    df['$'] = df['p'] * df['q']
    df.drop(['p', 'q'], axis='columns',inplace=True)
    #Calculate total $
    df_tot = df[df['type'].isin(price_types)].copy()
    df_tot = df_tot.groupby(['rb', 'timeslice', 'year'], sort=False, as_index=False).sum()
    df_tot['type'] = 'tot'
    df_tot['subtype'] = 'na'
    #Reformat quantities
    df_q = dfs['q']
    df_q.rename(columns={'q':'$'}, inplace=True)
    df_q['type'] = 'q_' + df_q['type']
    #Concatenate all dataframes
    df = pd.concat([df, df_tot, df_q],sort=False,ignore_index=True)
    return df

def pre_ng_price(dfs, **kw):
    #Apply inflation
    dfs['p']['p'] = inflate_series(dfs['p']['p'])
    #Join prices and quantities
    df = pd.merge(left=dfs['q'], right=dfs['p'], how='left', on=['census', 'year'], sort=False)
    df['p'].fillna(0, inplace=True)
    return df

# calculate storage power (GW) and energy (GWh) capacity
def calc_storage_cap(dfs, **kw):
    cap = dfs['cap'].copy()
    bcr = dfs['bcr'].copy()
    energy = dfs['energy'].copy()
    # aggregate vintages
    energy = energy.groupby(['tech', 'rb', 'year'])['Energy (GWh)'].sum().reset_index()
    # convert from MWh to GWh
    energy['Energy (GWh)'] =  energy['Energy (GWh)'] / 1e3
    # subset to storage techs
    cap_stor = cap.loc[cap['tech'].isin(energy['tech'].unique()),:].copy()
    # merge storage cap and battery/capacity ratio for relevant techs
    cap_stor = cap_stor.merge(bcr, on="tech", how='left')
    # assume missing bcr are 1 (mostly CSP)
    cap_stor['bcr'] = cap_stor['bcr'].fillna(1)
    # calculate storage power capacity using bcr, and convert from MW to GW
    cap_stor['Capacity (GW)'] = cap_stor['Capacity (GW)'] * cap_stor['bcr'] / 1e3
    # map CSP regions to BAs
    cap_stor = map_rs_to_rb(cap_stor)
    # merge in storage duration
    cap_stor = cap_stor.merge(energy, on=['tech', 'rb', 'year'])
    return cap_stor

def add_joint_locations_col(df, **kw):
    df[kw['new']] = df[kw['col1']] + '-' + df[kw['col2']]
    return df

def rgba2hex(rgba):
    """
    Input should be in fraction, not [0,255]
    """
    if len(rgba) == 4:
        r,g,b = rgba[:3]
    else:
        r,g,b = rgba

    ### r,g,b could be integers in [0,255] or floats in [0,1]
    if type(r) is int:
        scaler = 1
    else:
        scaler = 255

    hexcolor = '#{:02x}{:02x}{:02x}'.format(
        int(np.round(r*scaler)),
        int(np.round(g*scaler)),
        int(np.round(b*scaler)),
    ).upper()

    return hexcolor

def pre_runtime(dictin, **kw):
    """
    ### Use the code below to redefine the colormap when new scripts are added
    augur_start = df.index.tolist().index('ReEDS_Augur/A_prep_data.py')
    for i, row in enumerate(df.index):
        if i < augur_start:
            colors[row.lower()] = rgba2hex(plt.cm.tab20b(i))
        elif i < augur_start + 20:
            colors[row.lower()] = rgba2hex(plt.cm.tab20c(i-augur_start))
        else:
            colors[row.lower()] = rgba2hex(plt.cm.tab20(i-augur_start-20))
    """
    df = dictin['runtime'].copy()

    # Drop the git metadata columns
    df.columns = df.loc[2].values
    df.drop([0,1,2], inplace=True)

    # Drop blanks (end column)
    df = df[df.processtime.notnull()].copy()

    #Change dtypes
    df.processtime = df.processtime.astype(float)
    df.year = df.year.astype(int)

    #Reduce columns
    df = df[['year','process','processtime']].copy()

    #Convert from seconds to hours
    df['processtime'] = df['processtime']/3600

    #All processes with year 0 will be year 2000
    df['year'] = df['year'].replace(0, 2000)

    return df

def net_co2(dfs, **kw):
    co2 = dfs['emit'].copy()
    co2['Cumulative CO2e (MMton)'] = co2.groupby(['e','tech'])['CO2e (MMton)'].cumsum()
    # scale to million metric tonnes
    co2['CO2e (MMton)'] /= 1e6
    co2['Cumulative CO2e (MMton)'] /= 1e6
    return co2

# function to process health damage estimates
def process_health_damage(df, **kw):
    # convert to billion $ and inflate series
    df['Health damages (billion $)'] = inflate_series(df['Health damages (billion $)']) * 1e-9

    # convert emissions to thousand tonnes
    df['Emissions (thousand tonnes)'] = df['Emissions (thousand tonnes)'] * 1e-3

    # add column for discounted damages
    d = float(core.GL['widgets']['var_discount_rate'].value)
    y0 = int(core.GL['widgets']['var_pv_year'].value)
    df['Discounted health damages (billion $)'] = df['Health damages (billion $)'] / (1 + d)**(df['year'] - y0) 
    
    # add rows for any years missing data
    full_yrs = list(range(int(df['year'].min()), int(df['year'].max()) + 1))
    allRows = {'model': df['model'].unique().tolist(),
               'cr': df['cr'].unique().tolist(),
               'e': df['e'].unique().tolist(), 
               'year': full_yrs, 
               'rb': df['rb'].unique().tolist()}

    rows = product(*allRows.values())
    df_all = pd.DataFrame.from_records(rows, columns=allRows.keys())
    df_new = df.merge(df_all, how='outer', on=['model', 'cr', 'e', 'rb', 'year'])
    df_new['st'] = df_new['st'].interpolate(method="ffill")
    
    # sort by category and year
    df_new = df_new.sort_values(['model', 'cr', 'e', 'rb', 'year'])

    # interpolate any missing values 
    df_new = df_new.groupby(['model', 'cr', 'e', 'rb']).apply(lambda group: group.interpolate(method='ffill'))

    # sum over rb
    df_out = df_new.groupby(['model', 'cr', 'e', 'year'])[
            'Emissions (thousand tonnes)', 'Health damages (billion $)', 
            'Health damages (lives)', 'Discounted health damages (billion $)'
        ].sum().reset_index()

    # also sum over pollutant   
    df_poll_agg = df_new.groupby(['model', 'cr', 'year'])[
            'Health damages (billion $)', 'Health damages (lives)', 'Discounted health damages (billion $)'
        ].sum().reset_index()

    df_poll_agg.rename(columns={'Health damages (billion $)' : 'Total health damages (billion $)', 
                                'Health damages (lives)' : 'Total health damages (lives)',
                                'Discounted health damages (billion $)': 'Total discounted health damages (billion $)'}, inplace=True)

    df_out = df_out.merge(df_poll_agg, how="outer", on=["model", "cr", "year"])

    return df_out 

def process_social_costs(dfs, **kw):
    # get annual health damages
    health_costs = process_health_damage(dfs['health_damages'])

    # calculate national average electricity price
    system_costs = pre_avgprice(dfs, reg='National', shift_capital=True)

    # aggregate both costs types by year
    system_costs_agg = system_costs.groupby(['year']).agg({'Cost (Bil $)': 'sum', 'Discounted Cost (Bil $)': 'sum', 'q': 'mean'}).reset_index()
    system_costs_agg.rename(columns={'Cost (Bil $)' : 'Cost (Bil $)-system', 
                                    'Discounted Cost (Bil $)' : 'Discounted Cost (Bil $)-system'}, inplace=True)

    health_costs_agg = health_costs.groupby(['year', 'model', 'cr'])['Health damages (billion $)', 'Discounted health damages (billion $)'].sum().reset_index()
    health_costs_agg.rename(columns={'Health damages (billion $)' : 'Cost (Bil $)-health', 
                                    'Discounted health damages (billion $)' : 'Discounted Cost (Bil $)-health'}, inplace=True)

    # combine costs and melt
    costs_agg = system_costs_agg.merge(health_costs_agg, on="year", how="outer")
    cost_values = {'Cost (Bil $)-system': 'System cost', 'Discounted Cost (Bil $)-system': 'System cost', 'Cost (Bil $)-health':'System cost + health', 'Discounted Cost (Bil $)-health':'System cost + health'}

    costs_agg_long = pd.melt(costs_agg, id_vars=['year', 'model', 'cr', 'q'], value_vars=cost_values.keys(), var_name='cost', value_name= 'Cost (Bil $)')
    costs_agg_long['cost_type'] = costs_agg_long['cost'].map(cost_values)
    costs_agg_long['cost'] = costs_agg_long['cost'].str.replace('-.*', '', regex=True)
    costs_out = costs_agg_long.pivot(index=['year', 'model', 'cr', 'q', 'cost_type'], columns='cost', values='Cost (Bil $)').reset_index()

    # calculate average in $/MWh
    costs_out['Average cost ($/MWh)'] = costs_out['Cost (Bil $)'] * 1e9 / costs_out['q']
    costs_out['Discounted average cost ($/MWh)'] = costs_out['Discounted Cost (Bil $)'] * 1e9 / costs_out['q']

    return costs_out


def pre_spur(dfs, **kw):
    """
    Load spur-line parameters written by writesupplycurves.py and include spur lines
    in total transmission capacity. Currently only includes wind-ons and upv.
    """
    ### Load the inter-BA transmission MW-miles and convert to GW-miles
    tran_mi_out = dfs['tran_mi_out'].merge(
        dfs['tran_prm_mi_out'], on=['trtype','year'], how='outer')
    for col in ['Amount (GW-mi)', 'Trans cap, PRM (GW-mi)']:
        tran_mi_out[col] /= 1000
    ### If not including spur lines in total, stop here
    if ('ignore_spur' in kw) and kw['ignore_spur']:
        return tran_mi_out
    ### Load the spur-line distance for each PV/wind investment category
    spur_parameters = dfs['spur_parameters'].copy()
    ### Load the PV/wind investment, combine with spur-line distance
    cap_new_bin_out = (
        dfs['cap_new_bin_out']
        .groupby(['i','r','rscbin','year'], as_index=False).MW.sum()
    )
    cap_new_bin_out = (
        cap_new_bin_out
        .loc[cap_new_bin_out.i.str.startswith('wind-ons')
            | cap_new_bin_out.i.str.startswith('upv')]
        .merge(spur_parameters, on=['i','r','rscbin'], how='left')
    )
    ### Sum total spur-line GW-mi by year
    cap_new_bin_out['Amount (GW-mi)'] = (
        cap_new_bin_out['MW'] * cap_new_bin_out['dist_km']
        ### Convert from km to mi, and from MW to GW
        / 1.60934 / 1000
    )
    years = sorted(tran_mi_out.year.unique())
    spur = (
        cap_new_bin_out
        .groupby('year')['Amount (GW-mi)'].sum()
        ### cap_new_bin_out is annual additions, so fill missing years with zero
        ### then cumsum to get capacity
        .reindex(years).fillna(0)
        .cumsum()
        .reset_index().assign(trtype='spur')
    )
    ### Add spur lines to inter-BA transmission results
    tran_mi_out = pd.concat([tran_mi_out, spur], axis=0)

    return tran_mi_out

#---------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------

#2. Columns metadata. These are columns that are referenced in the Results section below.
#This is where joins, maps, and styles are applied for the columns.
#For 'style', colors are in hex, but descriptions are given (see http://www.color-hex.com/color-names.html).
#raw and display tech mappings for appending in tech_map.csv for coolingwatertech are given in:
#(https://github.nrel.gov/ReEDS/NEMS_Unit_Database_Water_Sources/blob/master/coolingtech/append2tech_map.csv)
columns_meta = {
    'tech':{
        'type':'string',
        'map': this_dir_path + '/in/reeds2/tech_map.csv',
        'style': this_dir_path + '/in/reeds2/tech_style.csv',
    },
    'class':{
        'type':'string',
        'map': this_dir_path + '/in/reeds2/class_map.csv',
        'style': this_dir_path + '/in/reeds2/class_style.csv',
    },
    'region':{
        'type':'string',
    },
    'rs':{
        'type':'string',
        'join': this_dir_path + '/in/reeds2/hierarchy.csv',
    },
    'rb':{
        'type':'string',
        'join': this_dir_path + '/in/reeds2/hierarchy.csv',
    },
    'day':{
        'type':'number',
        'filterable': True,
        'serieable' : True,
    },
    'month':{
        'type':'number',
        'filterable': True,
        'serieable' : True,
    },
    'year':{
        'type':'number',
        'filterable': True,
        'seriesable': True,
        'y-allow': False,
    },
    'iter':{
        'type':'string',
    },
    'icrb':{
        'type': 'string',
        'filterable': False,
        'seriesable': False,
    },
    'irbv':{
        'type': 'string',
        'filterable': False,
        'seriesable': False,
    },
    'cost_cat':{
        'type':'string',
        'map': this_dir_path + '/in/reeds2/cost_cat_map.csv',
        'style': this_dir_path + '/in/reeds2/cost_cat_style.csv',
    },
    'con_adj':{
        'type':'string',
        'map': this_dir_path + '/in/reeds2/con_adj_map.csv',
        'style': this_dir_path + '/in/reeds2/con_adj_style.csv',
    },
    'wst':{
        'type':'string',
        'map': this_dir_path + '/in/reeds2/wst_map.csv',
        'style': this_dir_path + '/in/reeds2/wst_style.csv',
    },
    'ctt':{
        'type':'string',
        'map': this_dir_path + '/in/reeds2/ctt_map.csv',
        'style': this_dir_path + '/in/reeds2/ctt_style.csv',
    },
    'process':{
        'type':'string',
        'style': this_dir_path + '/in/reeds2/process_style.csv',
    },
    'trtype':{
        'type':'string',
        'map': this_dir_path + '/in/reeds2/trtype_map.csv',
        'style': this_dir_path + '/in/reeds2/trtype_style.csv',
    },
}

#---------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------

#3. Results metadata. This is where all ReEDS results are defined. Parameters are read from gdx files, and
#are converted into pandas dataframes for pivoting. Preprocess functions may be used to perform additional manipulation.
#Note that multiple parameters may be read in for the same result (search below for 'sources')
#Presets may also be defined.

results_meta = collections.OrderedDict((
    ('Capacity National (Yearly)',
    	{'file': 'cap.csv',
    	'columns': ['tech', 'rb', 'year', 'month', 'day', 'hour', 'Capacity (GW)'],
    	'preprocess': [
       	{'func': sum_over_cols, 'args': {'drop_cols': ['rb', 'month', 'day', 'hour'], 'group_cols': ['tech', 'year']}},
        {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column': 'Capacity (GW)'}},
    	],
    	'index': ['tech', 'year'],
    	'presets': collections.OrderedDict((
        ('Stacked Bars', {'x': 'year', 'y': 'Capacity (GW)', 'series': 'tech', 'explode': 'scenario', 'chart_type': 'Bar', 'bar_width': '1'}),
    )),
    }
	),

    ('Capacity National (Monthly)',
        {'file': 'cap.csv',
        'columns': ['tech', 'rb', 'year', 'month', 'day', 'hour', 'Capacity (GW)'],
        'preprocess': [
            {'func': sum_over_months, 'args': {'drop_cols': ['rb', 'day', 'hour'], 'group_cols': ['tech', 'year'], 'month_col': 'month'}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column': 'Capacity (GW)'}},
        ],
        'index': ['tech', 'year', 'month'],
        'presets': collections.OrderedDict((
            ('Stacked Bars', {'x': 'month', 'y': 'Capacity (GW)', 'series': 'tech', 'explode': 'scenario', 'chart_type': 'Bar', 'bar_width': '1', 'filter': {'year': 'last'}}),
        )),
    }
    ),

('Capacity National (Daily)',
    {'file': 'cap.csv',
    'columns': ['tech', 'rb', 'year', 'month', 'day', 'hour', 'Capacity (GW)'],
    'preprocess': [
        {'func': sum_over_days, 'args': {'drop_cols': ['rb', 'hour'], 'group_cols': ['tech', 'year'], 'month_col': 'month', 'day_col': 'day'}},
        {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column': 'Capacity (GW)'}},
    ],
    'index': ['tech', 'year', 'month', 'day'],
    'presets': collections.OrderedDict((
        ('Stacked Bars', {'x': 'day', 'y': 'Capacity (GW)', 'series': 'tech', 'explode': 'scenario', 'chart_type': 'Bar', 'bar_width': '1', 'filter': {'year': 'last', 'month': 'last'}}),
    )),
    }
),

('Capacity National (Hourly)',
    {'file': 'cap.csv',
    'columns': ['tech', 'rb', 'year', 'month', 'day', 'hour', 'Capacity (GW)'],
    'preprocess': [
        {'func': sum_over_hours, 'args': {'drop_cols': ['rb'], 'group_cols': ['tech', 'year'], 'month_col': 'month', 'day_col': 'day', 'hour_col': 'hour'}},
        {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column': 'Capacity (GW)'}},
    ],
    'index': ['tech', 'year', 'month', 'day', 'hour'],
    'presets': collections.OrderedDict((
        ('Stacked Bars', {'x': 'hour', 'y': 'Capacity (GW)', 'series': 'tech', 'explode': 'scenario', 'chart_type': 'Bar', 'bar_width': '1', 'filter': {'year': 'last', 'month': 'last', 'day': 'last'}}),
    )),
    }
),
    ('Battery Load (Yearly)',
        {'file':'bat_load.csv',
        'columns': ['tech', 'rb', 'year', 'month','day','hour','Load (GW)'],
        'preprocess': [
            {'func': sum_over_cols, 'args': {'drop_cols': ['rb','month','day','hour'], 'group_cols': ['tech', 'year']}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Load (GW)'}},
        ],
        'index': ['tech', 'year'],
        'presets': collections.OrderedDict((
            ('Stacked Bars',{'x':'year', 'y':'Load (GW)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1'}),
        )),
        }
    ),

    ('Battery Load (Monthly)',
        {'file':'bat_load.csv',
        'columns': ['tech', 'rb', 'year', 'month','day','hour','Load (GW)'],
        'preprocess': [
            {'func': sum_over_months, 'args': {'drop_cols': ['rb','day','hour'], 'group_cols': ['tech', 'year'], 'month_col': 'month'}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Load (GW)'}},
        ],
        'index': ['tech', 'year','month'],
        'presets': collections.OrderedDict((
            ('Stacked Bars',{'x':'month', 'y':'Load (GW)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1','filter':{'year':'last'}}),
        )),
        }
    ),
    ('Battery Load (Daily)',
        {'file':'bat_load.csv',
        'columns': ['tech', 'rb', 'year', 'month','day','hour','Load (GW)'],
        'preprocess': [
            {'func': sum_over_days, 'args': {'drop_cols': ['rb','hour'], 'group_cols': ['tech', 'year'], 'month_col': 'month','day_col': 'day'}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Load (GW)'}},
        ],
        'index': ['tech', 'year','month','day'],
        'presets': collections.OrderedDict((
            ('Stacked Bars',{'x':'day', 'y':'Load (GW)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1','filter':{'year':'last','month':'last'}}),
        )),
        }
    ),
    ('Battery Load (Hourly)',
        {'file':'bat_load.csv',
        'columns': ['tech', 'rb', 'year', 'month','day','hour','Load (GW)'],
        'preprocess': [
            {'func': sum_over_hours, 'args': {'drop_cols': ['rb'], 'group_cols': ['tech', 'year'], 'month_col': 'month', 'day_col': 'day', 'hour_col':'hour'}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Load (GW)'}},
        ],
        'index': ['tech', 'year','month','day','hour'],
        'presets': collections.OrderedDict((
            ('Stacked Bars',{'x':'hour', 'y':'Load (GW)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1','filter':{'year':'last','month':'last','day':'last'}}),
        )),
        }
    ),

    ('Generation National (TWh)',
        {'file':'gen_ann.csv',
        'columns': ['tech', 'rb', 'year', 'month','day','hour','Generation (TWh)'],
        'preprocess': [
            {'func': sum_over_cols, 'args': {'drop_cols': ['rb','month','day','hour'], 'group_cols': ['tech', 'year']}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Generation (TWh)'}},
        ],
        'index': ['tech', 'year'],
        'presets': collections.OrderedDict((
            ('Stacked Area',{'x':'year', 'y':'Generation (TWh)', 'series':'tech', 'explode':'scenario', 'chart_type':'Area'}),
            ('Stacked Bars',{'x':'year', 'y':'Generation (TWh)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1'}),
            ('Stacked Bars Gen Frac',{'x':'year', 'y':'Generation (TWh)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1', 'adv_op':'Ratio', 'adv_col':'tech', 'adv_col_base':'Total'}),
            ('Explode By Tech',{'x':'year', 'y':'Generation (TWh)', 'series':'scenario', 'explode':'tech', 'chart_type':'Line'}),
        )),
        }
    ),

    ('Generation National Month',
        {'file':'gen_ann.csv',
        'columns': ['tech', 'rb', 'year','month','day','hour','Generation (TWh)'],
        'preprocess': [
            {'func': sum_over_months, 'args': {'drop_cols': ['rb','day','hour'], 'group_cols': ['tech', 'year'], 'month_col': 'month'}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Generation (TWh)'}},
        ],
        'index': ['tech','year','month'],
        'presets': collections.OrderedDict((
            ('Stacked Bars',{'x':'month', 'y':'Generation (TWh)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1','filter': {'year':'last'}}),
        )),
        }
     ),
    
    ('Generation National Day',
        {'file':'gen_ann.csv',
        'columns': ['tech', 'rb', 'year','month','day','hour','Generation (TWh)'],
        'preprocess': [
            {'func': sum_over_days, 'args': {'drop_cols': ['rb','hour'], 'group_cols': ['tech', 'year'], 'month_col': 'month','day_col': 'day'}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Generation (TWh)'}},
        ],
        'index': ['tech','year','month','day'],
        'presets': collections.OrderedDict((
            ('Stacked Bars',{'x':'day', 'y':'Generation (TWh)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1','filter':{'year':'last','month':'last'}}),
        )),
        }
     ),

    ('Generation National hour',
        {'file':'gen_ann.csv',
        'columns': ['tech', 'rb', 'year','month','day','hour','Generation (TWh)'],
        'preprocess': [
            {'func': sum_over_hours, 'args': {'drop_cols': ['rb'], 'group_cols': ['tech', 'year'], 'month_col': 'month', 'day_col': 'day', 'hour_col':'hour'}},
            {'func': scale_column, 'args': {'scale_factor': 1e-6, 'column':'Generation (TWh)'}},
        ],
        'index': ['tech','year','month','day','hour'],
        'presets': collections.OrderedDict((
            ('Stacked Bars',{'x':'hour', 'y':'Generation (TWh)', 'series':'tech', 'explode':'scenario', 'chart_type':'Bar', 'bar_width':'1', 'filter':{'year':'last','month':'last','day':'last'}}),
        )),
        }
     ),

('CO2 Emissions by Generator (Yearly)',
    {
        'file': 'emit_r.csv',
        'columns': ['e', 'rb', 'year', 'month', 'day', 'hour', 'Emissions (tonne)'],
        'preprocess': [
            {'func': sum_over_cols, 'args': {'drop_cols': ['rb', 'month', 'day', 'hour'], 'group_cols': ['e', 'year']}},
        ],
        'index': ['e', 'year'],
        'presets': collections.OrderedDict((
            ('Stacked Bars', {
                'x': 'year',
                'y': 'Emissions (tonne)',
                'series': 'e',
                'explode': 'scenario',
                'chart_type': 'Bar',
                'bar_width': '1'
            }),
        )),
    }
),
('CO2 Emissions National (Yearly)',
    {
        'file': 'emit_r.csv',
        'columns': ['e', 'rb', 'year', 'month', 'day', 'hour', 'Emissions (tonne)'],
        'preprocess': [
            {'func': sum_over_cols, 'args': {'drop_cols': ['rb', 'month', 'day', 'hour'], 'group_cols': ['e', 'year']}},
        ],
        'index': ['e', 'year'],
        'presets': collections.OrderedDict((
            ('Stacked Bars', {
                'x': 'year',
                'y': 'Emissions (tonne)',
                'series': 'e',
                'explode': 'scenario',
                'chart_type': 'Bar',
                'bar_width': '1',
                'filter': {'e': ['co2']}
            }),
        )),
    }
),

('CO2 Emissions National (Monthly)',
    {
        'file': 'emit_r.csv',
        'columns': ['e', 'rb', 'year', 'month', 'day', 'hour', 'Emissions (tonne)'],
        'preprocess': [
            {
                'func': sum_over_months,
                'args': {
                    'drop_cols': ['rb', 'day', 'hour'],
                    'group_cols': ['e', 'year'],
                    'month_col': 'month'
                }
            },
        ],
        'index': ['e', 'year', 'month'],
        'presets': collections.OrderedDict((
            ('Stacked Bars', {
                'x': 'month',
                'y': 'Emissions (tonne)',
                'series': 'e',
                'explode': 'scenario',
                'chart_type': 'Bar',
                'bar_width': '1',
                'filter': {'year': 'last', 'e': ['co2']}
            }),
        )),
    }
),

('CO2 Emissions National (Daily)',
    {
        'file': 'emit_r.csv',
        'columns': ['e', 'rb', 'year', 'month', 'day', 'hour', 'Emissions (tonne)'],
        'preprocess': [
            {
                'func': sum_over_days,
                'args': {
                    'drop_cols': ['rb', 'hour'],
                    'group_cols': ['e', 'year'],
                    'month_col': 'month',
                    'day_col': 'day'
                }
            },
        ],
        'index': ['e', 'year', 'month', 'day'],
        'presets': collections.OrderedDict((
            ('Stacked Bars', {
                'x': 'day',
                'y': 'Emissions (tonne)',
                'series': 'e',
                'explode': 'scenario',
                'chart_type': 'Bar',
                'bar_width': '1',
                'filter': {'year': 'last', 'month': 'last', 'e': ['co2']}
            }),
        )),
    }
),

('CO2 Emissions National (Hourly)',
    {
        'file': 'emit_r.csv',
        'columns': ['e', 'rb', 'year', 'month', 'day', 'hour', 'Emissions (tonne)'],
        'preprocess': [
            {
                'func': sum_over_hours,
                'args': {
                    'drop_cols': ['rb'],
                    'group_cols': ['e', 'year'],
                    'month_col': 'month',
                    'day_col': 'day',
                    'hour_col': 'hour'
                }
            },
        ],
        'index': ['e', 'year', 'month', 'day', 'hour'],
        'presets': collections.OrderedDict((
            ('Stacked Bars', {
                'x': 'hour',
                'y': 'Emissions (tonne)',
                'series': 'e',
                'explode': 'scenario',
                'chart_type': 'Bar',
                'bar_width': '1',
                'filter': {'year': 'last', 'month': 'last', 'day': 'last', 'e': ['co2']}
            }),
        )),
    }
),

))

#Sort alphabetically
results_meta = collections.OrderedDict(sorted(results_meta.items()))

"""
This module is used to generate overview from db files for equity, fixed_income and hard-coded cash equivalent data.

    Original Author: Mark D
    Date created: 01/05/2019
    Date Modified: 11/04/2021
    Python Version: 3.7

Note:
    This module depend on following third party library:
     - pandas v0.25.0
    to install:
        pip3 install pandas

Examples:
    -- Generate allocation summary based on Investment Type:
        from src.overview_generator import SummaryTool as SummaryTool
        this_instance = SummaryTool()
        this_instance.generate_allocation_report_type()

    -- Generate allocation summary based on Broker:
        from src.overview_generator import SummaryTool as SummaryTool
        this_instance = SummaryTool()
        this_instance.generate_allocation_report_account()

    -- Generate allocation summary for Equity ETF:
        from src.overview_generator import SummaryTool as SummaryTool
        this_instance = SummaryTool()
        this_instance.generate_allocation_report_equity_etf()

    -- Generate allocation summary for Fixed Income ETF:
        from src.overview_generator import SummaryTool as SummaryTool
        this_instance = SummaryTool()
        this_instance.generate_allocation_report_fixed_etf()

    -- Generate Mature Calender for Fixed Income:
        from src.overview_generator import SummaryTool as SummaryTool
        this_instance = SummaryTool()
        this_instance.generate_mature_calender()

"""

from datetime import datetime
import pandas as pd
import numpy as np
from .logger import UseLogging
from .eq_SQLite_utility import SQLiteRequest as eq_SQLiteRequest
from .fixed_SQLite_utility import FixedSQLiteRequest as fixed_SQLiteRequest


this_fixed_income_etf = {
    'VGSH': ['Vanguard Short-Term Treasury Index Fund',
             'Short-Term Fixed Income', 'Treasury'
             ],
    'VGIT': ['Vanguard Intermediate-Term Treasury Index Fund',
             'Intermediate-Term Fixed Income', 'Treasury'
             ],
    'VGLT': ['Vanguard Long-Term Treasury Index Fund',
             'Long-Term Fixed Income', 'Treasury'
             ],
    'VCSH': ['Vanguard Short-Term Corporate Bond Index Fund',
             'Short-Term Fixed Income', 'Corporate'
             ],
    'VCIT': ['Vanguard Intermediate-Term Corporate Bond Index Fund',
             'Intermediate-Term Fixed Income', 'Corporate'
             ],
    'VCLT': ['Vanguard Long-Term Corporate Bond Index Fund',
             'Long-Term Fixed Income', 'Corporate'
             ],
    'BSV': ['Vanguard Short-Term Bond Index Fund',
            'Short-Term Fixed Income', 'Blend'
            ],
    'BIV': ['Vanguard Intermediate-Term Bond Index Fund',
            'Intermediate-Term Fixed Income', 'Blend'
            ],
    'BLV': ['Vanguard Long-Term Bond Index Fund',
            'Long-Term Fixed Income', 'Blend'
            ],
    'VTIP': ['Vanguard Short-Term Inflation Protected Securities',
             'Short-Term Fixed Income', 'Inflation-protected'
             ]
}

this_equity_etf = {
    'VOO': ['Vanguard S&P 500 Index Fund',
            'Large-Cap', 'Blend'
            ],
    'VIG': ['Vanguard Dividend Appreciation Index Fund',
            'Large-Cap', 'Div Appreciate'
            ],
    'VTV': ['Vanguard Value Index Fund',
            'Large-Cap', 'Value'
            ],
    'VUG': ['Vanguard Growth Index Fund',
            'Large-Cap', 'Growth'
            ],
    'VO': ['Vanguard Mid-Cap Index Fund',
           'Mid-Cap', 'Blend'
           ],
    'VOE': ['Vanguard Mid-Cap Value Index Fund',
            'Mid-Cap', 'Value'
            ],
    'VOT': ['Vanguard Mid-Cap Growth Index Fund',
            'Mid-Cap', 'Growth'
            ],
    'VB': ['Vanguard Small-Cap Index Fund',
           'Small-Cap', 'Blend'
           ],
    'VBR': ['Vanguard Small-Cap Value Index Fund',
            'Small-Cap', 'Value'
            ],
    'VBK': ['Vanguard Small-Cap Growth Index Fund',
            'Small-Cap', 'Growth'
            ],
    'VGT': ['Vanguard Information Technology Index Fund',
            'Sector-Specific Equity', 'Information Technology'
            ],
    'VHT': ['Vanguard Health Care Index Fund',
            'Sector-Specific Equity', 'Health Care'
            ],
    'VDC': ['Vanguard Consumer Staples Index Fund',
            'Sector-Specific Equity', 'Consumer Staples'
            ],
    'VCR': ['Vanguard Consumer Discretionary Index Fund',
            'Sector-Specific Equity', 'Consumer Discretionary'
            ],
    'VNQ': ['Vanguard Real Estate Index Fund',
            'Alternative', 'REITs'
            ],
    'VPU': ['Vanguard Utility Index Fund',
            'Sector-Specific Equity', 'Utility'
            ],
    'VIS': ['Vanguard Industrial Index Fund',
            'Sector-Specific Equity', 'Industrial'
            ],
    'VIGI': ['Vanguard International Dividend Appreciate ETF',
             'Foreign Equity', 'Large Blend'
             ],
    'VEA': ['Vanguard FTSE Developed Markets ETF',
            'Foreign Equity', 'Developed Markets'
            ],
    'VWO': ['Vanguard FTSE Emerging Markets ETF',
            'Foreign Equity', 'Emerging Markets']
}


class SummaryTool(object):
    """
    The :class: SummaryTool can be used to get summary from all SQLite db files.
    """
    def __init__(self):
        """
        constructor for :class: SummaryTool.
        """
        _logger_ref = UseLogging(__name__)
        self.logger = _logger_ref.use_loggers('investment_management')
        self.eq_db_file = 'databases/equity.db'
        self.fixed_db_file = 'databases/fixed_income.db'
        self.other_investment_file = 'databases/others.json'

    def _get_eq_transactions_data(self):
        """Read data from :table: transactions in SQLite equity.db.

        Returns: :object: Pandas dataframe.

        """
        self.logger.info(f'Attempt to retrieve data from :table: transactions in {self.eq_db_file}...')
        try:
            _this_instance = eq_SQLiteRequest(self.eq_db_file)
            _this_output = _this_instance.get_table_transactions()
            df = pd.DataFrame(_this_output,
                              columns=['ID', 'SYMBOL', 'TYPE', 'DATE', 'DOLLARS', 'UNITS', 'INVESTMENT_TYPE',
                                       'DESCRIPTION', 'ACCOUNT', 'TOTAL_DOLLARS']
                              )
            return df
        except Exception as e:
            self.logger.error(f'Failed to retrieve data from :table: transactions in {self.eq_db_file} -> '+str(e))
            raise e

    def _get_eq_positions_data(self):
        """Read data from :view: positions in SQLite equity.db.

        Returns: :object: Pandas dataframe.

        """
        self.logger.info(f'Attempt to retrieve data from :view: positions in {self.eq_db_file}...')
        try:
            _this_instance = eq_SQLiteRequest(self.eq_db_file)
            _this_output = _this_instance.get_view_positions()
            df = pd.DataFrame(_this_output,
                              columns=['SYMBOL', 'DESCRIPTION', 'INVESTMENT_TYPE', 'COST_DOLLARS', 'DOLLARS',
                                       'UNITS', 'LAST_UPDATED', 'MKT_VALUE', 'GAIN_PER_SHARE', 'GAIN_TOTAL',
                                       'GAIN_PERCENTAGE']
                              )
            return df
        except Exception as e:
            self.logger.error(f'Failed to retrieve data from :view: positions in {self.eq_db_file} -> '+str(e))
            raise e

    def _get_fixed_positions_data(self):
        """Read data from :view: positions in SQLite fixed_income.db.

        Returns: :object: Pandas dataframe.

        """
        self.logger.info(f'Attempt to retrieve data from :view: positions in {self.fixed_db_file}...')
        try:
            _this_instance = fixed_SQLiteRequest(self.fixed_db_file)
            _this_output = _this_instance.get_view_positions_fixed()
            df = pd.DataFrame(_this_output,
                              columns=['NAME', 'SYMBOL', 'INVESTMENT_TYPE', 'UNITS', 'FACE_VALUE', 'TOTAL_DOLLARS',
                                       'ADD_DATE', 'END_DATE', 'TOTAL_COST', 'RETURN_RATE', 'RETURN_DOLLARS',
                                       'IS_MATURED']
                              )
            return df
        except Exception as e:
            self.logger.error(f'Failed to retrieve data from :view: positions in {self.fixed_db_file} -> '+str(e))
            raise e

    def _get_fixed_transactions_data(self):
        """Read data from :table: transactions in SQLite fixed_income.db.

        Returns: :object: Pandas dataframe.

        """
        self.logger.info(f'Attempt to retrieve data from :table: transactions in {self.fixed_db_file}...')
        try:
            _this_instance = fixed_SQLiteRequest(self.fixed_db_file)
            _this_output = _this_instance.get_table_transactions_fixed()
            df = pd.DataFrame(_this_output,
                              columns=['ID', 'NAME', 'SYMBOL', 'INVESTMENT_TYPE', 'UNITS', 'FACE_VALUE',
                                       'TOTAL_DOLLARS', 'ADD_DATE', 'END_DATE', 'TOTAL_COST', 'APR', 'YTM',
                                       'ACCOUNT']
                              )
            return df
        except Exception as e:
            self.logger.error(f'Failed to retrieve data from :table: transactions in {self.fixed_db_file} -> '+str(e))
            raise e

    def _get_other_investment_information(self):
        """Read data from hard-coded cash_equivalent csv file.

        Returns: :object: Pandas dataframe.

        """
        self.logger.info(f'Attempt to retrieve data from {self.other_investment_file}...')
        try:
            df_input = pd.read_json(self.other_investment_file, orient='records')
            df_output = pd.DataFrame(df_input,
                                     columns=['DESCRIPTION', 'MAJOR_TYPE', 'MINOR_TYPE', 'DOLLARS', 'ACCOUNT'])
            return df_output
        except Exception as e:
            self.logger.error(f'Failed to retrieve data from {self.other_investment_file} -> '+str(e))
            raise e

    def generate_allocation_report_type(self):
        """Get allocation report based on investment type.

        Return: :object: Pandas dataframe.

        """

        def _set_major_investment_type_equity(v_symbol):
            """Set MAJOR_TYPE based on SYMBOL for Equity entries"""
            if v_symbol.upper() in list(this_fixed_income_etf.keys()):
                out_major_type = 'FIXED_INCOME'
            else:
                out_major_type = 'EQUITY'
            return out_major_type

        self.logger.info('Generating Allocation report based on investment_type ...')
        try:
            df_eq = self._get_eq_positions_data()[['SYMBOL', 'INVESTMENT_TYPE', 'MKT_VALUE']]
            df_fixed = self._get_fixed_positions_data()[['SYMBOL', 'INVESTMENT_TYPE', 'TOTAL_DOLLARS']]
            df_other_investment = self._get_other_investment_information()[['DESCRIPTION', 'MAJOR_TYPE',
                                                                            'MINOR_TYPE', 'DOLLARS']]
            df_eq.columns = ['SYMBOL', 'MINOR_TYPE', 'DOLLARS']
            df_fixed.columns = ['SYMBOL', 'MINOR_TYPE', 'DOLLARS']
            df_other_investment.columns = ['SYMBOL', 'MAJOR_TYPE', 'MINOR_TYPE', 'DOLLARS']
            self.logger.info('Applying logic to build :column: MAJOR_TYPE ...')
            df_eq['MAJOR_TYPE'] = df_eq['SYMBOL'].apply(lambda x: _set_major_investment_type_equity(x))
            df_fixed['MAJOR_TYPE'] = 'FIXED_INCOME'
            self.logger.info('Preparing allocation summary ...')
            df_combined = pd.concat([
                df_eq[['MAJOR_TYPE', 'MINOR_TYPE', 'DOLLARS']],
                df_fixed[['MAJOR_TYPE', 'MINOR_TYPE', 'DOLLARS']],
                df_other_investment[['MAJOR_TYPE', 'MINOR_TYPE', 'DOLLARS']]
            ])
            df_allocation_major_type = df_combined['DOLLARS'].groupby(
                df_combined['MAJOR_TYPE']).sum().reset_index(name='MAJOR_TOTAL_DOLLARS')
            df_allocation_major_type['MAJOR_ALLOCATION'] = (
                    df_allocation_major_type['MAJOR_TOTAL_DOLLARS'] /
                    df_allocation_major_type['MAJOR_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_minor_type = df_combined['DOLLARS'].groupby(
                [df_combined['MAJOR_TYPE'],
                 df_combined['MINOR_TYPE']]).sum().reset_index(name='MINOR_TOTAL_DOLLARS')
            df_allocation_minor_type['MINOR_ALLOCATION'] = (
                    df_allocation_minor_type['MINOR_TOTAL_DOLLARS'] /
                    df_allocation_minor_type['MINOR_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_report = df_allocation_major_type.merge(df_allocation_minor_type, on='MAJOR_TYPE')
            df_allocation_report = df_allocation_report.sort_values(
                ['MAJOR_ALLOCATION', 'MINOR_ALLOCATION'], ascending=False)
            df_output = df_allocation_report[['MAJOR_TYPE', 'MAJOR_TOTAL_DOLLARS', 'MAJOR_ALLOCATION',
                                              'MINOR_TYPE', 'MINOR_TOTAL_DOLLARS', 'MINOR_ALLOCATION'
                                              ]].append(
                {'MAJOR_TYPE': 'TOTAL',
                    'MAJOR_TOTAL_DOLLARS': df_allocation_report['MINOR_TOTAL_DOLLARS'].sum(),
                    'MAJOR_ALLOCATION': 100.0,
                    'MINOR_TYPE': '',
                    'MINOR_TOTAL_DOLLARS': float('nan'),
                    'MINOR_ALLOCATION': float('nan')}, ignore_index=True)
            self.logger.info('Formatting columns with float data type ...')
            df_output['MAJOR_ALLOCATION'] = df_output['MAJOR_ALLOCATION'].map('{:.0f}%'.format)
            df_output['MINOR_ALLOCATION'] = df_output['MINOR_ALLOCATION'].map('{:.0f}%'.format)
            df_output['MAJOR_TOTAL_DOLLARS'] = df_output['MAJOR_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            df_output['MINOR_TOTAL_DOLLARS'] = df_output['MINOR_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            self.logger.info('Making Pandas Dataframe easy to read ...')
            df_output['MAJOR_IS_DUPLICATE'] = df_output[
                ['MAJOR_TYPE', 'MAJOR_TOTAL_DOLLARS', 'MAJOR_ALLOCATION']].duplicated()

            def _func_clean_major_allocation(row):
                if row[6] is True:
                    return ''
                else:
                    return row[2]

            def _func_clean_major_total_dollars(row):
                if row[6] is True:
                    return ''
                else:
                    return row[1]

            df_output['MAJOR_ALLOCATION'] = df_output.apply(_func_clean_major_allocation, axis=1)
            df_output['MAJOR_TOTAL_DOLLARS'] = df_output.apply(_func_clean_major_total_dollars, axis=1)
            return df_output.drop(columns=['MAJOR_IS_DUPLICATE'])
        except Exception as e:
            self.logger.error('Failed to generate allocation report based on investment_type  -> '+str(e))
            raise e

    def generate_mature_calender(self):
        """Get mature calender for fixed income.

        Returns: :object: Pandas dataframe.

        """
        self.logger.info('Generating Mature Calender for fixed income investment ...')
        try:
            df_fixed = self._get_fixed_positions_data()[['SYMBOL', 'END_DATE', 'TOTAL_DOLLARS', 'RETURN_RATE']]
            self.logger.info('Updating Pandas Dataframe column label ...')
            df_fixed.columns = ['SYMBOL', 'MATURE_DATE', 'DOLLARS', 'RETURN_RATE']
            df_fixed['RETURN'] = df_fixed['DOLLARS']*df_fixed['RETURN_RATE']
            self.logger.info('Updating :column: MATURE_DATE format from YYYY-MM-DD to YYYY-MM ...')
            df_fixed['MATURE_DATE'] = df_fixed['MATURE_DATE'].apply(
                lambda x: datetime.strptime(datetime.strptime(x, '%Y-%m-%d').strftime('%Y-%m'), '%Y-%m'))
            self.logger.info('Calculating total Dollars, Counts, Returns, and average Yield for each MATURE_DATE ...')
            df_mature_calender = df_fixed.groupby(df_fixed['MATURE_DATE']).agg(
                {'DOLLARS': ['sum', 'count'],
                 'RETURN': ['sum'],
                 'SYMBOL': [lambda x: ', '.join([y.replace('n/a', 'CD-renew') for y in x])]
                 }).reset_index()
            df_mature_calender.columns = ['MATURE_DATE', 'TOTAL_DOLLARS', 'TOTAL_COUNT', 'YIELD', 'SYMBOL_REF']
            df_mature_calender['YIELD'] = df_mature_calender['YIELD']/df_mature_calender['TOTAL_DOLLARS']*100
            self.logger.info('Filtering Pandas Dataframe to exclude rows with MATURE_DATE < CURRENT_MONTH ...')
            _current_month = datetime.strptime(datetime.today().strftime('%Y-%m'), '%Y-%m')
            _delete_row = df_mature_calender[df_mature_calender['MATURE_DATE'] < _current_month].index
            df_mature_calender = df_mature_calender.drop(_delete_row)
            df_mature_calender['MATURE_DATE'] = df_mature_calender['MATURE_DATE'].apply(lambda x: x.strftime('%Y-%m'))
            df_output = df_mature_calender.sort_values('MATURE_DATE', ascending=True).reset_index()
            self.logger.info('Formatting columns with float data type ...')
            df_output['TOTAL_DOLLARS'] = df_output['TOTAL_DOLLARS'].map('${:,.0f}'.format)
            df_output['YIELD'] = df_output['YIELD'].map('{:.2f}%'.format)
            return df_output
        except Exception as e:
            self.logger.error('Failed to generate Mature Calender for fixed income investment  -> '+str(e))
            raise e

    def generate_allocation_report_account(self):
        """Get allocation report based on Broker(Account).

        Returns: :object: Pandas dataframe.

        """
        self.logger.info('Generating Allocation report based on ACCOUNT ...')
        try:
            df_eq_positions = self._get_eq_positions_data()[['SYMBOL', 'DOLLARS']]
            df_eq_transactions = self._get_eq_transactions_data()[['SYMBOL', 'TYPE', 'UNITS', 'ACCOUNT']]
            df_eq_transactions['UNITS'].loc[df_eq_transactions['TYPE'] == 'SELL'] = -1*df_eq_transactions['UNITS']
            df_eq_aggregated_transactions = df_eq_transactions.groupby(
                ['ACCOUNT', 'SYMBOL'])['UNITS'].sum().reset_index(name='TOTAL_UNITS')
            df_eq_aggregated_transactions.drop(
                df_eq_aggregated_transactions[df_eq_aggregated_transactions['TOTAL_UNITS'] <= 0].index,
                inplace=True)
            df_eq_combined = df_eq_aggregated_transactions.merge(df_eq_positions, left_on='SYMBOL', right_on='SYMBOL')
            df_eq_combined['TOTAL_DOLLARS'] = df_eq_combined['DOLLARS']*df_eq_combined['TOTAL_UNITS']
            df_eq = df_eq_combined.groupby(['ACCOUNT'])['TOTAL_DOLLARS'].sum().reset_index(name='DOLLARS')
            df_fixed = self._get_fixed_transactions_data()[['TOTAL_DOLLARS', 'END_DATE', 'ACCOUNT']]
            df_fixed.columns = ['DOLLARS', 'END_DATE', 'ACCOUNT']
            df_fixed['END_DATE'] = df_fixed['END_DATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
            _current_month = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
            _delete_row = df_fixed[df_fixed['END_DATE'] < _current_month].index
            df_fixed = df_fixed.drop(_delete_row)
            df_other_investment = self._get_other_investment_information()[['ACCOUNT', 'DOLLARS']]
            df_combined = pd.concat([
                df_eq[['ACCOUNT', 'DOLLARS']],
                df_fixed[['ACCOUNT', 'DOLLARS']],
                df_other_investment[['ACCOUNT', 'DOLLARS']]
            ])
            df_allocation_account = df_combined['DOLLARS'].groupby(
                df_combined['ACCOUNT']).sum().reset_index(name='TOTAL_DOLLARS')
            df_allocation_account['ALLOCATION'] = (
                    df_allocation_account['TOTAL_DOLLARS'] / df_allocation_account['TOTAL_DOLLARS'].sum() * 100)
            df_output = df_allocation_account.sort_values('ALLOCATION', ascending=False)
            self.logger.info('Formatting columns with float data type ...')
            df_output['ALLOCATION'] = df_output['ALLOCATION'].map('{:.0f}%'.format)
            df_output['TOTAL_DOLLARS'] = df_output['TOTAL_DOLLARS'].map('${:,.0f}'.format)
            return df_output
        except Exception as e:
            self.logger.error('Failed to generate Allocation report based on ACCOUNT  -> '+str(e))
            raise e

    def generate_allocation_report_equity_etf(self):
        """Get allocation report for Equity ETF.

        Return: :object: Pandas DataFrame.

        """
        def _set_equity_etf_asset_class(v_symbol, v_index):
            """Set Equity ETF Asset Class based on SYMBOL"""
            if v_symbol.upper() in list(this_equity_etf.keys()):
                out_asset_class = this_equity_etf.get(v_symbol.upper())[v_index]
            else:
                out_asset_class = 'Others'
            return out_asset_class

        self.logger.info('Generating Allocation report for Equity ETF ...')
        try:
            # Disable pandas warning message
            pd.options.mode.chained_assignment = None
            df_eq = self._get_eq_positions_data()[['SYMBOL', 'INVESTMENT_TYPE', 'MKT_VALUE']]
            df_eq.columns = ['SYMBOL', 'INVESTMENT_TYPE', 'DOLLARS']
            df_combined = df_eq[((df_eq['INVESTMENT_TYPE'] == 'ETF') | (df_eq['INVESTMENT_TYPE'] == 'etf')) &
                                (~df_eq['SYMBOL'].isin(list(this_fixed_income_etf.keys())))]
            df_combined['ASSET_CLASS'] = df_combined['SYMBOL'].apply(lambda x: _set_equity_etf_asset_class(x, 1))
            df_combined['SUBCLASS'] = df_combined['SYMBOL'].apply(lambda x: _set_equity_etf_asset_class(x, 2))
            df_allocation_class = df_combined['DOLLARS'].groupby(
                df_combined['ASSET_CLASS']).sum().reset_index(name='ASSET_CLASS_TOTAL_DOLLARS')
            df_allocation_class['ASSET_CLASS_ALLOCATION'] = (
                    df_allocation_class['ASSET_CLASS_TOTAL_DOLLARS'] /
                    df_allocation_class['ASSET_CLASS_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_subclass = df_combined['DOLLARS'].groupby(
                [df_combined['ASSET_CLASS'],
                 df_combined['SUBCLASS']]).sum().reset_index(name='SUBCLASS_TOTAL_DOLLARS')
            df_allocation_subclass['SUBCLASS_ALLOCATION'] = (
                    df_allocation_subclass['SUBCLASS_TOTAL_DOLLARS'] /
                    df_allocation_subclass['SUBCLASS_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_report = df_allocation_class.merge(df_allocation_subclass, on='ASSET_CLASS')
            df_allocation_report = df_allocation_report.sort_values(
                ['ASSET_CLASS_ALLOCATION', 'SUBCLASS_ALLOCATION'], ascending=False)
            df_output = df_allocation_report[['ASSET_CLASS', 'ASSET_CLASS_TOTAL_DOLLARS', 'ASSET_CLASS_ALLOCATION',
                                              'SUBCLASS', 'SUBCLASS_TOTAL_DOLLARS', 'SUBCLASS_ALLOCATION'
                                              ]].append(
                {'ASSET_CLASS': 'TOTAL',
                 'ASSET_CLASS_TOTAL_DOLLARS': df_allocation_report['SUBCLASS_TOTAL_DOLLARS'].sum(),
                 'ASSET_CLASS_ALLOCATION': 100.0,
                 'SUBCLASS': '',
                 'SUBCLASS_TOTAL_DOLLARS': float('nan'),
                 'SUBCLASS_ALLOCATION': float('nan')}, ignore_index=True)
            self.logger.info('Formatting columns with float data type ...')
            df_output['ASSET_CLASS_ALLOCATION'] = df_output['ASSET_CLASS_ALLOCATION'].map('{:.0f}%'.format)
            df_output['SUBCLASS_ALLOCATION'] = df_output['SUBCLASS_ALLOCATION'].map('{:.0f}%'.format)
            df_output['ASSET_CLASS_TOTAL_DOLLARS'] = df_output['ASSET_CLASS_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            df_output['SUBCLASS_TOTAL_DOLLARS'] = df_output['SUBCLASS_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            self.logger.info('Making Pandas Dataframe easy to read ...')
            df_output['ASSET_CLASS_IS_DUPLICATE'] = df_output[
                ['ASSET_CLASS', 'ASSET_CLASS_TOTAL_DOLLARS', 'ASSET_CLASS_ALLOCATION']].duplicated()

            def _func_clean_major_allocation(row):
                if row[6] is True:
                    return ''
                else:
                    return row[2]

            def _func_clean_major_total_dollars(row):
                if row[6] is True:
                    return ''
                else:
                    return row[1]

            df_output['ASSET_CLASS_ALLOCATION'] = df_output.apply(_func_clean_major_allocation, axis=1)
            df_output['ASSET_CLASS_TOTAL_DOLLARS'] = df_output.apply(_func_clean_major_total_dollars, axis=1)
            return df_output.drop(columns=['ASSET_CLASS_IS_DUPLICATE'])
        except Exception as e:
            self.logger.error('Failed to generate allocation report for Equity ETF -> '+str(e))
            raise e

    def generate_allocation_report_fixed_etf(self):
        """Get allocation report for Fixed ETF.

        Return: :object: Pandas DataFrame.

        """
        def _set_fixed_etf_asset_class(v_symbol, v_index):
            """Set Fixed Income ETF Asset Class based on SYMBOL"""
            if v_symbol.upper() in list(this_fixed_income_etf.keys()):
                out_asset_class = this_fixed_income_etf.get(v_symbol.upper())[v_index]
            else:
                out_asset_class = 'Others'
            return out_asset_class

        self.logger.info('Generating Allocation report for Fixed Income ETF ...')
        try:
            pd.options.mode.chained_assignment = None
            df_fixed = self._get_eq_positions_data()[['SYMBOL', 'INVESTMENT_TYPE', 'MKT_VALUE']]
            df_fixed.columns = ['SYMBOL', 'INVESTMENT_TYPE', 'DOLLARS']
            df_combined = df_fixed[((df_fixed['INVESTMENT_TYPE'] == 'ETF') | (df_fixed['INVESTMENT_TYPE'] == 'etf')) &
                                   (df_fixed['SYMBOL'].isin(list(this_fixed_income_etf.keys())))]
            df_combined['ASSET_CLASS'] = df_combined['SYMBOL'].apply(lambda x: _set_fixed_etf_asset_class(x, 1))
            df_combined['SUBCLASS'] = df_combined['SYMBOL'].apply(lambda x: _set_fixed_etf_asset_class(x, 2))
            df_allocation_class = df_combined['DOLLARS'].groupby(
                df_combined['ASSET_CLASS']).sum().reset_index(name='ASSET_CLASS_TOTAL_DOLLARS')
            df_allocation_class['ASSET_CLASS_ALLOCATION'] = (
                    df_allocation_class['ASSET_CLASS_TOTAL_DOLLARS'] /
                    df_allocation_class['ASSET_CLASS_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_subclass = df_combined['DOLLARS'].groupby(
                [df_combined['ASSET_CLASS'],
                 df_combined['SUBCLASS']]).sum().reset_index(name='SUBCLASS_TOTAL_DOLLARS')
            df_allocation_subclass['SUBCLASS_ALLOCATION'] = (
                    df_allocation_subclass['SUBCLASS_TOTAL_DOLLARS'] /
                    df_allocation_subclass['SUBCLASS_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_report = df_allocation_class.merge(df_allocation_subclass, on='ASSET_CLASS')
            df_allocation_report = df_allocation_report.sort_values(
                ['ASSET_CLASS_ALLOCATION', 'SUBCLASS_ALLOCATION'], ascending=False)
            df_output = df_allocation_report[['ASSET_CLASS', 'ASSET_CLASS_TOTAL_DOLLARS', 'ASSET_CLASS_ALLOCATION',
                                              'SUBCLASS', 'SUBCLASS_TOTAL_DOLLARS', 'SUBCLASS_ALLOCATION'
                                              ]].append(
                {'ASSET_CLASS': 'TOTAL',
                 'ASSET_CLASS_TOTAL_DOLLARS': df_allocation_report['SUBCLASS_TOTAL_DOLLARS'].sum(),
                 'ASSET_CLASS_ALLOCATION': 100.0,
                 'SUBCLASS': '',
                 'SUBCLASS_TOTAL_DOLLARS': float('nan'),
                 'SUBCLASS_ALLOCATION': float('nan')}, ignore_index=True)
            self.logger.info('Formatting columns with float data type ...')
            df_output['ASSET_CLASS_ALLOCATION'] = df_output['ASSET_CLASS_ALLOCATION'].map('{:.0f}%'.format)
            df_output['SUBCLASS_ALLOCATION'] = df_output['SUBCLASS_ALLOCATION'].map('{:.0f}%'.format)
            df_output['ASSET_CLASS_TOTAL_DOLLARS'] = df_output['ASSET_CLASS_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            df_output['SUBCLASS_TOTAL_DOLLARS'] = df_output['SUBCLASS_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            self.logger.info('Making Pandas Dataframe easy to read ...')
            df_output['ASSET_CLASS_IS_DUPLICATE'] = df_output[
                ['ASSET_CLASS', 'ASSET_CLASS_TOTAL_DOLLARS', 'ASSET_CLASS_ALLOCATION']].duplicated()

            def _func_clean_major_allocation(row):
                if row[6] is True:
                    return ''
                else:
                    return row[2]

            def _func_clean_major_total_dollars(row):
                if row[6] is True:
                    return ''
                else:
                    return row[1]

            df_output['ASSET_CLASS_ALLOCATION'] = df_output.apply(_func_clean_major_allocation, axis=1)
            df_output['ASSET_CLASS_TOTAL_DOLLARS'] = df_output.apply(_func_clean_major_total_dollars, axis=1)
            return df_output.drop(columns=['ASSET_CLASS_IS_DUPLICATE'])
        except Exception as e:
            self.logger.error('Failed to generate allocation report for Fixed Income ETF -> '+str(e))
            raise e

    def generate_allocation_report_equity_stock(self):
        """Get allocation report for Equity Stock.

        Return: :object: Pandas DataFrame.

        """
        self.logger.info('Generating Allocation report for Equity Stock ...')
        try:
            pd.options.mode.chained_assignment = None
            df_eq = self._get_eq_positions_data()[['SYMBOL', 'DESCRIPTION', 'INVESTMENT_TYPE', 'MKT_VALUE']]
            df_eq.columns = ['SYMBOL', 'DESCRIPTION', 'INVESTMENT_TYPE', 'DOLLARS']
            df_allocation_report = df_eq[((df_eq['INVESTMENT_TYPE'] == 'STOCK') | (df_eq['INVESTMENT_TYPE'] == 'stock'))
                                         & ~(df_eq['SYMBOL'].isin(['GPRO']))]
            df_allocation_report['STOCK_ALLOCATION'] = (
                    df_allocation_report['DOLLARS'] / df_allocation_report['DOLLARS'].sum() * 100)
            df_allocation_report = df_allocation_report.sort_values(['STOCK_ALLOCATION'], ascending=False)[
                ['SYMBOL', 'DESCRIPTION', 'DOLLARS', 'STOCK_ALLOCATION']]
            df_output = df_allocation_report[['SYMBOL', 'DESCRIPTION', 'DOLLARS', 'STOCK_ALLOCATION'
                                              ]].append(
                {'SYMBOL': 'TOTAL',
                 'DESCRIPTION': 'N/A',
                 'DOLLARS': df_allocation_report['DOLLARS'].sum(),
                 'STOCK_ALLOCATION': 100.0
                 }, ignore_index=True)
            self.logger.info('Formatting columns with float data type ...')
            df_output['STOCK_ALLOCATION'] = df_output['STOCK_ALLOCATION'].map('{:.2f}%'.format)
            df_output['DOLLARS'] = df_output['DOLLARS'].map('${:,.0f}'.format)
            return df_output
        except Exception as e:
            self.logger.error('Failed to generate allocation report for Equity Stock -> '+str(e))
            raise e

    def generate_allocation_report_etf_w_account(self, v_account):
        """Get allocation report for Equity ETF.

        Return: :object: Pandas DataFrame.

        """
        def _set_etf_asset_class(v_symbol, v_index):
            """Set ETF Asset Class based on SYMBOL"""
            if v_symbol.upper() in list(this_equity_etf.keys()):
                out_asset_class = this_equity_etf.get(v_symbol.upper())[v_index]
            elif v_symbol.upper() in list(this_fixed_income_etf.keys()):
                out_asset_class = this_fixed_income_etf.get(v_symbol.upper())[v_index]
            else:
                out_asset_class = 'Others'
            return out_asset_class

        self.logger.info('Generating Allocation report for Equity ETF, group by account ...')
        try:
            pd.options.mode.chained_assignment = None
            df_transactions = self._get_eq_transactions_data()[['SYMBOL', 'ACCOUNT', 'TYPE', 'UNITS']]
            df_transactions['ADJUSTED_UNITS'] = np.where(df_transactions['TYPE'] == 'BUY',
                                                         df_transactions['UNITS'],
                                                         -1 * df_transactions['UNITS'])
            df_positions = self._get_eq_positions_data()[['SYMBOL', 'DESCRIPTION', 'INVESTMENT_TYPE', 'DOLLARS']]
            df_other_investment = self._get_other_investment_information()[['DESCRIPTION', 'MAJOR_TYPE', 'ACCOUNT',
                                                                            'DOLLARS']]
            df_cash_equivalent = df_other_investment[(df_other_investment['ACCOUNT'] == v_account) &
                                                     (df_other_investment['MAJOR_TYPE'] == 'Cash Equivalent'
                                                      )].groupby(['MAJOR_TYPE'])['DOLLARS'].sum().\
                reset_index(name='TOTAL_DOLLARS')
            df_cash_equivalent.columns = ['ASSET_CLASS', 'DOLLARS']
            df_eq = df_transactions.groupby(['SYMBOL', 'ACCOUNT'])['ADJUSTED_UNITS'].sum().\
                reset_index(name='TOTAL_UNITS').query('TOTAL_UNITS > 0').\
                join(df_positions.set_index('SYMBOL'), on='SYMBOL')
            df_eq['TOTAL_DOLLARS'] = df_eq['TOTAL_UNITS'] * df_eq['DOLLARS']
            df_final = df_eq[(df_eq['ACCOUNT'] == v_account)][['SYMBOL', 'INVESTMENT_TYPE', 'TOTAL_DOLLARS']]
            df_final.columns = ['SYMBOL', 'INVESTMENT_TYPE', 'DOLLARS']
            df_combined = df_final[((df_final['INVESTMENT_TYPE'] == 'ETF') | (df_final['INVESTMENT_TYPE'] == 'etf'))]
            df_combined['ASSET_CLASS'] = df_combined['SYMBOL'].apply(lambda x: _set_etf_asset_class(x, 1))
            df_combined['SUBCLASS'] = df_combined['SYMBOL'].apply(lambda x: _set_etf_asset_class(x, 2))
            if df_cash_equivalent.shape[0] > 0:
                df_cash_equivalent['SYMBOL'] = 'n/a'
                df_cash_equivalent['INVESTMENT_TYPE'] = 'Cash'
                df_cash_equivalent['SUBCLASS'] = 'Cash'
                df_combined = df_combined.append(df_cash_equivalent[[
                    'SYMBOL', 'INVESTMENT_TYPE', 'DOLLARS', 'ASSET_CLASS', 'SUBCLASS']], ignore_index=True)
            df_allocation_class = df_combined['DOLLARS'].groupby(
                df_combined['ASSET_CLASS']).sum().reset_index(name='ASSET_CLASS_TOTAL_DOLLARS')
            df_allocation_class['ASSET_CLASS_ALLOCATION'] = (
                    df_allocation_class['ASSET_CLASS_TOTAL_DOLLARS'] /
                    df_allocation_class['ASSET_CLASS_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_subclass = df_combined['DOLLARS'].groupby(
                [df_combined['ASSET_CLASS'],
                 df_combined['SUBCLASS']]).sum().reset_index(name='SUBCLASS_TOTAL_DOLLARS')
            df_allocation_subclass['SUBCLASS_ALLOCATION'] = (
                    df_allocation_subclass['SUBCLASS_TOTAL_DOLLARS'] /
                    df_allocation_subclass['SUBCLASS_TOTAL_DOLLARS'].sum() * 100)
            df_allocation_report = df_allocation_class.merge(df_allocation_subclass, on='ASSET_CLASS')
            df_allocation_report = df_allocation_report.sort_values(
                ['ASSET_CLASS_ALLOCATION', 'SUBCLASS_ALLOCATION'], ascending=False)
            df_output = df_allocation_report[['ASSET_CLASS', 'ASSET_CLASS_TOTAL_DOLLARS', 'ASSET_CLASS_ALLOCATION',
                                              'SUBCLASS', 'SUBCLASS_TOTAL_DOLLARS', 'SUBCLASS_ALLOCATION'
                                              ]].append(
                {'ASSET_CLASS': 'TOTAL',
                 'ASSET_CLASS_TOTAL_DOLLARS': df_allocation_report['SUBCLASS_TOTAL_DOLLARS'].sum(),
                 'ASSET_CLASS_ALLOCATION': 100.0,
                 'SUBCLASS': '',
                 'SUBCLASS_TOTAL_DOLLARS': float('nan'),
                 'SUBCLASS_ALLOCATION': float('nan')}, ignore_index=True)
            self.logger.info('Formatting columns with float data type ...')
            df_output['ASSET_CLASS_ALLOCATION'] = df_output['ASSET_CLASS_ALLOCATION'].map('{:.2f}%'.format)
            df_output['SUBCLASS_ALLOCATION'] = df_output['SUBCLASS_ALLOCATION'].map('{:.2f}%'.format)
            df_output['ASSET_CLASS_TOTAL_DOLLARS'] = df_output['ASSET_CLASS_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            df_output['SUBCLASS_TOTAL_DOLLARS'] = df_output['SUBCLASS_TOTAL_DOLLARS'].map('${:,.0f}'.format)
            self.logger.info('Making Pandas Dataframe easy to read ...')
            df_output['ASSET_CLASS_IS_DUPLICATE'] = df_output[
                ['ASSET_CLASS', 'ASSET_CLASS_TOTAL_DOLLARS', 'ASSET_CLASS_ALLOCATION']].duplicated()

            def _func_clean_major_allocation(row):
                if row[6] is True:
                    return ''
                else:
                    return row[2]

            def _func_clean_major_total_dollars(row):
                if row[6] is True:
                    return ''
                else:
                    return row[1]

            df_output['ASSET_CLASS_ALLOCATION'] = df_output.apply(_func_clean_major_allocation, axis=1)
            df_output['ASSET_CLASS_TOTAL_DOLLARS'] = df_output.apply(_func_clean_major_total_dollars, axis=1)
            return df_output.drop(columns=['ASSET_CLASS_IS_DUPLICATE'])
        except Exception as e:
            self.logger.error('Failed to generate allocation report for Equity ETF group by Account -> '+str(e))
            raise e

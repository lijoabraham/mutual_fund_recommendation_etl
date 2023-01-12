'''
Example of web scraping using Python and BeautifulSoup.
The script will loop through a defined number of pages to extract mutual funds data.
'''

import datetime
import json
import os
import re
from urllib.parse import urlparse
import time
import requests
from requests import Session
from bs4 import BeautifulSoup
import concurrent.futures
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.mysql.dml import Insert
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings

disable_warnings(InsecureRequestWarning)

import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'./packages.zip'))
from dependencies import db

            
class MFScrapper(object):

    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)

        self._search_url = config['search_url']
        self._file_name = config['file_name']
        self._basic_detail_url = config['basic_detail_url']
        self._cred_rating_url = config['cred_rating_url']
        self._output_path = config['output_path']
        self._mf_list_num = config['mf_list_num']
        self._cnt = 1
        self._get_session()
        self._db_session = db._connect_to_database(config)
        self._commit_size=5
        self._commit_count=0
        self._row_values = []
        self._total_cnt = 0

    def _get_session(self):
        time.sleep(1)
        session = Session()
        headers = self._get_headers()
        # Add headers
        session.headers.update(headers)
        self._session = session
        return session

    def _get_file_name(self):
        cur_date = datetime.datetime.now()
        date_name = cur_date.strftime("%d-%m-%Y")
        cur_file_name = self._file_name.format(date_name)
        return self._output_path + cur_file_name

    def write_output(self, row):
        with open(self._get_file_name(), 'a') as toWrite:
            json.dump(row, toWrite)
            toWrite.write('\n')

    # Staticmethod to extract a domain out of a normal link
    @staticmethod
    def parse_url(url):
        parsed_uri = urlparse(url)
        return parsed_uri

    def cast_value(self, value, type='float'):
        try:
            if value == '--':
                return 0
            elif type == 'int':
                return int(value)
            else:
                return float(value)
        except:
            return 0

    def _get_headers(self):
        host = self.parse_url(self._search_url)
        headers = {
                    # 'Host': host.netloc,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
                   }
        return headers

    def get_src_html(self, url):
        try:
            if self._cnt %  10 == 0:
                self._get_session()
            response = self._session.get(url, headers = self._get_headers(), verify=False)
            # response = requests.get(url, headers = self._get_headers())
            print("URL : {} \nTime taken : {}".format(url, response.elapsed.total_seconds()))
            self._cnt+=1
        except requests.exceptions.RequestException as e:
            print(e)
            exit()
        return response

    def get_listing_data(self):
        '''
        scrap search data from the page
        '''
        # prepare headers
        print("Listing URL : " + self._search_url)
        # fetching the url, raising error if operation fails
        response = self.get_src_html(self._search_url)
        listings = []
        #
        # loop through the table, get data from the columns
        if response.status_code == 200:
            soup = BeautifulSoup(response.json()['html_data'], "html.parser")
            tbody = soup.find("tbody")
            if tbody:
                links = tbody.find_all("tr")
                if len(links) != 0:
                    listings = links[:self._mf_list_num]
        return listings[:10]

    def parse_fund_details(self, html):
        try:
            details = {}
            sub_rows = html.find_all("td")
            if sub_rows:
                # print('\n Processing Item : ' + str(i))
                details['name'] = sub_rows[0].find('a').get_text()
                details['link'] = sub_rows[0].find("a")['href']
                # if '15737' not in details['link']:
                #     return
                details['id'] = details['link'].split("/")[2]
                rating = sub_rows[1].find('img')
                if rating:
                    details['rating'] = self.cast_value(rating.attrs.get('alt').replace(' star', ''), 'int')
                else:
                    details['rating'] = 0
                details['category_id'] = sub_rows[4].find('a')['href'].split("/")[4]
                details['category'] = sub_rows[4].find('a').attrs['data-original-title']
                details['category_code'] = sub_rows[4].find('a').get_text()
                details['asset_value'] = self.cast_value(sub_rows[8].get_text().replace(",", ""), 'int')
                details['expense_ratio'] = self.cast_value(sub_rows[6].get_text())

                fund_details = self.get_fund_details(details['link'])
                details['fund_house'] = fund_details['fund_house'] if 'fund_house' in fund_details else ''
                details['risk'] = fund_details['risk'] if 'risk' in fund_details else ''
                details['risk_grade'] = fund_details['risk_grade'] if 'risk_grade' in fund_details else ''
                details['return_grade'] = fund_details['return_grade'] if 'return_grade' in fund_details else ''
                details['fund_growth'] = fund_details['fund_growth'] if 'fund_growth' in fund_details else {}
                details['category_growth'] = fund_details['category_growth'] if 'category_growth' in fund_details else {}
                details['fund_credit_rating'] = fund_details['fund_credit_rating'] if 'fund_credit_rating' in fund_details else {}
                details['cat_credit_rating'] = fund_details['cat_credit_rating'] if 'cat_credit_rating' in fund_details else {}
                details['top_holdings'] = fund_details['top_holdings'] if 'top_holdings' in fund_details else []
                details['fund_portfolio_agg'] = fund_details['fund_portfolio_agg'] if 'fund_portfolio_agg' in fund_details else []
                details['cat_portfolio_agg'] = fund_details['cat_portfolio_agg'] if 'cat_portfolio_agg' in fund_details else []
                details['fund_risk'] = fund_details['fund_risk'] if 'fund_risk' in fund_details else {}
                details['category_risk'] = fund_details['category_risk'] if 'category_risk' in fund_details else {}
                self.push_data(details)
                # self.write_output(details)
        except Exception as e:
            print(e, details['link'], fund_details)
            exit()

    def get_fund_details(self, detail_url):
        '''
        scrap detail page data from the page
        '''
        fund_id = detail_url.split("/")[2]
        host = self.parse_url(self._search_url)
        detail_url = '{uri.scheme}://{uri.netloc}'.format(uri=host) + detail_url
        print(" Detail URL : " + detail_url)
        
        fund_details = {}
        if fund_id:
            fund_basic_details = self._get_basic_details(fund_id)
            trailing_returns = self._get_trailing_returns(fund_id)
            credit_rating = self._get_credit_rating(fund_id)
            top_holding = self._get_top_holdings(fund_id)
            risk_measures = self._get_risk_measures(fund_id)

            fund_details = {**fund_basic_details, **trailing_returns, **credit_rating, **top_holding, **risk_measures}
        return fund_details

    def _get_basic_details(self, fund_id):
        try:
            temp = []
            details = {}
            basic_detail_url = f"{self._basic_detail_url}/{fund_id}/?tab=other"
            response = self.get_src_html(basic_detail_url)
        
            if response.status_code == 200:
                src_html = BeautifulSoup(response.json()['tab_html'], "html.parser")
            
                # loop through the table, get data from the columns
                basic_details_table = src_html.find_all('ul', class_='investment-details-list')
                if basic_details_table is not None:
                    temp = basic_details_table[1].find_all("h4")
                    if temp:
                        fund_house = temp[0].find('a').get_text()
                        details['fund_house'] = re.sub('[^0-9a-zA-Z]+', ' ', fund_house).strip()
                        details['risk'] = temp[4].get_text()
                        details['risk_grade'] = temp[8].get_text().strip()
                        details['return_grade'] = temp[9].get_text().strip()
            return details
        except Exception as e:
            print(" Function : {}\n Exception : {}".format('_get_basic_details', e))

    def _get_trailing_returns(self, fund_id):
        try:
            trailing_returns = {}
            ret_data = []
            trailing_returns_url = f"{self._basic_detail_url}/{fund_id}/?tab=performance"
            response = self.get_src_html(trailing_returns_url)
        
            if response.status_code == 200:
                src_html = BeautifulSoup(response.json()['tab_html'], "html.parser")
                trailing_returns_table = src_html.find('table', attrs={'id': 'return_over_time_table'})
                if trailing_returns_table is not None:
                    table_body = trailing_returns_table.find('tbody')
                    rows = table_body.find_all('tr')
                    if rows:
                        for row in rows:
                            cols = row.find_all('td')
                            cols = [ele.text.strip() for ele in cols]
                            ret_data.append([ele for ele in cols if ele])

                    fund_data = {}
                    fund_data['3m'] = self.cast_value(ret_data[0][4])
                    fund_data['6m'] = self.cast_value(ret_data[0][5])
                    fund_data['1y'] = self.cast_value(ret_data[0][6])
                    fund_data['3y'] = self.cast_value(ret_data[0][7])
                    fund_data['5y'] = self.cast_value(ret_data[0][8])
                    fund_data['7y'] = self.cast_value(ret_data[0][9])
                    fund_data['10y'] = self.cast_value(ret_data[0][10])
                    trailing_returns['fund_growth'] = fund_data

                    cat_ret = {}
                    cat_ret['3m'] = self.cast_value(ret_data[2][4])
                    cat_ret['6m'] = self.cast_value(ret_data[2][5])
                    cat_ret['1y'] = self.cast_value(ret_data[2][6])
                    cat_ret['3y'] = self.cast_value(ret_data[2][7])
                    cat_ret['5y'] = self.cast_value(ret_data[2][8])
                    cat_ret['7y'] = self.cast_value(ret_data[2][9])
                    cat_ret['10y'] = self.cast_value(ret_data[2][10])
                    trailing_returns['category_growth'] = cat_ret

            return trailing_returns
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_trailing_returns', e))

    def _get_credit_rating(self, fund_id):
        try:
            credit_rating_data = {}
            cred_rating_url = self._cred_rating_url + fund_id
            print(" Credit Rating URL : " + cred_rating_url)

            response = self.get_src_html(cred_rating_url)
            i = 0
            cred_rating = {'fund':{}, 'cat':{}}
            if response.status_code == 200:
                for cat in response.json()['categories']:
                    cat = cat.replace('+','plus').replace('/','and').replace(' ','_').lower()
                    cred_rating['fund'][cat] = self.cast_value(response.json()['series'][0]['data'][i])
                    cred_rating['cat'][cat] = self.cast_value(response.json()['series'][1]['data'][i])
                    i += 1
                
            fund_credit_rating = {}
            fund_credit_rating['aaa'] = cred_rating['fund']['aaa'] if 'aaa' in cred_rating['fund'] else 0
            fund_credit_rating['a1plus'] = cred_rating['fund']['a1plus'] if 'a1plus' in cred_rating['fund'] else 0
            fund_credit_rating['sov'] = cred_rating['fund']['sov'] if 'sov' in cred_rating['fund'] else 0
            fund_credit_rating['cash_equivalent'] = cred_rating['fund']['cash_equivalent'] if 'cash_equivalent' in cred_rating['fund'] else 0
            fund_credit_rating['aa'] = cred_rating['fund']['aa'] if 'aa' in cred_rating['fund'] else 0
            fund_credit_rating['a_and_below'] = cred_rating['fund']['a_and_below'] if 'a_and_below' in cred_rating['fund'] else 0
            fund_credit_rating['unrated_and_others'] = cred_rating['fund']['unrated_and_others'] if 'unrated_and_others' in cred_rating['fund'] else 0

            cat_credit_rating = {}
            cat_credit_rating['aaa'] = cred_rating['cat']['aaa'] if 'aaa' in cred_rating['cat'] else 0
            cat_credit_rating['a1plus'] = cred_rating['cat']['a1plus'] if 'a1plus' in cred_rating['cat'] else 0
            cat_credit_rating['sov'] = cred_rating['cat']['sov'] if 'sov' in cred_rating['cat'] else 0
            cat_credit_rating['cash_equivalent'] = cred_rating['cat']['cash_equivalent'] if 'cash_equivalent' in cred_rating['cat'] else 0
            cat_credit_rating['aa'] = cred_rating['cat']['aa'] if 'aa' in cred_rating['cat'] else 0
            cat_credit_rating['a_and_below'] = cred_rating['cat']['a_and_below'] if 'a_and_below' in cred_rating['cat'] else 0
            cat_credit_rating['unrated_and_others'] = cred_rating['cat']['unrated_and_others'] if 'unrated_and_others' in cred_rating['cat'] else 0

            credit_rating_data['fund_credit_rating'] = fund_credit_rating
            credit_rating_data['cat_credit_rating'] = cat_credit_rating
            return credit_rating_data
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_credit_rating', e))

    def _get_top_holdings(self, fund_id):
        try:
            top_holdings_data = {}
            top_holdings = []
            i = 0
            top_holdings_url = f"{self._basic_detail_url}/{fund_id}/?tab=fund-portfolio"
            response = self.get_src_html(top_holdings_url)
        
            if response.status_code == 200:
                src_html = BeautifulSoup(response.json()['tab_html'], "html.parser")
                top_holdings_table = src_html.find('table', id='company-holding_table_debt')
                if top_holdings_table:
                    for rows in top_holdings_table.find('tbody').find_all("tr"):
                        if i > 10:
                            break
                        sub_rows = rows.find_all("td")
                        if sub_rows:
                            top_holdings.append(sub_rows[0].get_text().strip())
                            i += 1
                top_holdings_data['top_holdings'] = top_holdings 

                agg_data = []
                portfolio_aggregates = src_html.find('div', id='portfolioEquityTab').find('table','table')
                if portfolio_aggregates:
                    table_body = portfolio_aggregates.find('tbody')
                    rows = table_body.find_all('tr')
                    if rows:
                        for row in rows:
                            cols = row.find_all('td')
                            cols = [ele.text.strip() for ele in cols]
                            agg_data.append([ele for ele in cols if ele])
                fund_data = {}
                fund_data['num_securities'] = self.cast_value(agg_data[0][1] if '1' in agg_data[0] else 0)
                fund_data['modified_duration'] = self.cast_value(agg_data[1][1] if '1' in agg_data[1] else 0)
                fund_data['average_maturity'] = self.cast_value(agg_data[2][1] if '1' in agg_data[2] else 0)
                fund_data['ytm'] = self.cast_value(agg_data[3][1] if '1' in agg_data[3] else 0)
                fund_data['avg_cr'] = self.cast_value(agg_data[4][1] if '1' in agg_data[4] else 0)
                top_holdings_data['fund_portfolio_agg'] = fund_data

                cat_data = {}
                cat_data['num_securities'] = self.cast_value(agg_data[0][2] if '2' in agg_data[0] else 0)
                cat_data['modified_duration'] = self.cast_value(agg_data[1][2] if '2' in agg_data[1] else 0)
                cat_data['average_maturity'] = self.cast_value(agg_data[2][2] if '2' in agg_data[2] else 0)
                cat_data['ytm'] = self.cast_value(agg_data[3][2] if '2' in agg_data[3] else 0)
                cat_data['avg_cr'] = self.cast_value(agg_data[4][2] if '2' in agg_data[4] else 0)
                top_holdings_data['cat_portfolio_agg'] = cat_data
      
            return top_holdings_data
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_top_holdings', e))

    def _get_risk_measures(self, fund_id):
        try:
            risk_measures = {}
            ret_data = []
            risk_measures_url = f"{self._basic_detail_url}/{fund_id}/?tab=risk"
            response = self.get_src_html(risk_measures_url)
        
            if response.status_code == 200:
                src_html = BeautifulSoup(response.json()['tab_html'], "html.parser")
                risk_measures_table = src_html.find('div', attrs={'class': 'risk-table-view'}).find('table')
                if risk_measures_table is not None:
                    table_body = risk_measures_table.find('tbody')
                    rows = table_body.find_all('tr')
                    if rows:
                        for row in rows:
                            cols = row.find_all('td')
                            cols = [ele.text.strip() for ele in cols]
                            ret_data.append([ele for ele in cols if ele])
                    # print(ret_data[0][1])
                    # exit()
                    fund_risk = {}
                    fund_risk['mean'] = self.cast_value(ret_data[0][1])
                    fund_risk['std_dev'] = self.cast_value(ret_data[0][2])
                    fund_risk['sharpe'] = self.cast_value(ret_data[0][3])
                    fund_risk['sortino'] = self.cast_value(ret_data[0][4])
                    fund_risk['beta'] = self.cast_value(ret_data[0][5])
                    fund_risk['alpha'] = self.cast_value(ret_data[0][6])
                    risk_measures['fund_risk'] = fund_risk
                    #
                    cat_risk = {}
                    cat_risk['mean'] = self.cast_value(ret_data[2][1])
                    cat_risk['std_dev'] = self.cast_value(ret_data[2][2])
                    cat_risk['sharpe'] = self.cast_value(ret_data[2][3])
                    cat_risk['sortino'] = self.cast_value(ret_data[2][4])
                    cat_risk['beta'] = self.cast_value(ret_data[2][5])
                    cat_risk['alpha'] = self.cast_value(ret_data[2][6])
                    risk_measures['category_risk'] = cat_risk
            return risk_measures
        except Exception as e:
            print("Function : {}\n Exception : {}".format('_get_risk_measures', e))

    def extract(self):
        # Get mf data
        listings = self.get_listing_data()
        self._total_cnt = len(listings)
        for fund in listings:
            self.parse_fund_details(fund)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        #     executor.map(self.parse_fund_details, listings)
        print("Listings fetched successfully.")

    def push_data(self,row):
        if row:
            self._row_values.append(row)
            self._commit_count += 1
            self._total_cnt -= 1
            if self._commit_count == self._commit_size:
                self.insert_or_update()
                self._commit_count, self._row_values = 0, []
                return
            if self._row_values and self._total_cnt == 0:
                self.insert_or_update()
                return
            
    def insert_or_update(self):
        self.insert_or_update_mf_data()
        self.insert_or_update_mf_growth()
        self.insert_or_update_mf_credit_rating()
        self.insert_or_update_mf_risk()
        self.insert_or_update_mf_category_data()
        self.insert_or_update_mf_category_growth()
        self.insert_or_update_mf_category_risk()
        self.insert_or_update_mf_cat_credit_rating()
        self.insert_or_update_mf_portfolio_agg()
        self.insert_or_update_mf_cat_portfolio_agg()
        self._db_session.commit()
    
    def insert_or_update_mf_data(self):
        metadata = MetaData()
        mf_data = Table('mf_data', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r[i.name] for i in mf_data.columns if i.name not in ['time_added','time_modified']}
            data['top_holdings'] = json.dumps(data['top_holdings'])
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_data).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_growth(self):
        metadata = MetaData()
        mf_growth = Table('mf_growth', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['fund_growth'][i.name] for i in mf_growth.columns if i.name not in ['mf_id','time_added','time_modified']}
            data['mf_id'] = r['id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_growth).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_credit_rating(self):
        metadata = MetaData()
        mf_credit_rating = Table('mf_credit_rating', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['fund_credit_rating'][i.name] for i in mf_credit_rating.columns if i.name not in ['mf_id','time_added','time_modified']}
            data['mf_id'] = r['id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_credit_rating).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_risk(self):
        metadata = MetaData()
        mf_risk = Table('mf_risk', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['fund_risk'][i.name] for i in mf_risk.columns if i.name not in ['mf_id','time_added','time_modified']}
            data['mf_id'] = r['id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_risk).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_portfolio_agg(self):
        metadata = MetaData()
        mf_growth = Table('mf_portfolio_agg', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['fund_portfolio_agg'][i.name] for i in mf_growth.columns if i.name not in ['mf_id','time_added','time_modified']}
            data['mf_id'] = r['id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_growth).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_category_data(self):
        metadata = MetaData()
        mf_category_data = Table('mf_category_data', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r[i.name] for i in mf_category_data.columns if i.name not in ['id','time_added','time_modified']}
            data['id'] = r['category_id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_category_data).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_category_growth(self):
        metadata = MetaData()
        mf_category_growth = Table('mf_category_growth', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['category_growth'][i.name] for i in mf_category_growth.columns if i.name not in ['mf_cat_id','time_added','time_modified']}
            data['mf_cat_id'] = r['category_id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_category_growth).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_cat_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_category_risk(self):
        metadata = MetaData()
        mf_category_risk = Table('mf_category_risk', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['category_risk'][i.name] for i in mf_category_risk.columns if i.name not in ['mf_cat_id','time_added','time_modified']}
            data['mf_cat_id'] = r['category_id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_category_risk).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_cat_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_cat_credit_rating(self):
        metadata = MetaData()
        mf_credit_rating = Table('mf_cat_credit_rating', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['cat_credit_rating'][i.name] for i in mf_credit_rating.columns if i.name not in ['mf_cat_id','time_added','time_modified']}
            data['mf_cat_id'] = r['category_id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_credit_rating).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_cat_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    def insert_or_update_mf_cat_portfolio_agg(self):
        metadata = MetaData()
        mf_growth = Table('mf_cat_portfolio_agg', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        final_data = []
        for r in self._row_values:
            data = {i.name:r['cat_portfolio_agg'][i.name] for i in mf_growth.columns if i.name not in ['mf_cat_id','time_added','time_modified']}
            data['mf_cat_id'] = r['category_id']
            data['time_added'] = datetime.now()
            final_data.append(data)
        insert_stmt = Insert(mf_growth).values(final_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_cat_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
    
    


# Main function
if __name__ == "__main__":
    try:
        cur_date = datetime.now()
        date_name = cur_date.strftime("%d-%m-%Y")
        print(f"\n************** Fetching details for {date_name} ********************\n")
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'./configs/scrapper.json')
        mf = MFScrapper(config_path)
        mf.extract()
        # data = mf.get_fund_details('www.valueresearchonline.com/funds/15737/uti-ultra-short-term-fund-direct')
        # print(data)
    except Exception as e:
            print(e)
            exit()

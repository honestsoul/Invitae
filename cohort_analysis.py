

import datetime
import pandas as pd
import numpy as np
import webbrowser
from IPython.display import display, HTML
from datetime import timedelta, date


class Cohort(object):
    '''
    Class for cohort analysis
    '''
    
    def __init__(self,customer_file,order_file,time_zone = None):
        self.customer_file_path = customer_file
        self.order_file_path = order_file
        self.time_zone =time_zone
        
    def __calculate_week_range(self,diff):
        '''This method will calculate the 
           week range such as 0-6,7-14 days etc.
           input => (timedelta such as 0 day 1 day)
        '''
        if diff < 0:
            return '0'
        if len(str(diff))>1:
            if diff%10 != 0:
                zing = int(str(diff)[0])+1
            else:
                zing = int(str(diff)[0])
        else:
            zing = 1
        #Week range such as 0-6 days, 7-14 days
        week = str(0+7*(zing-1)) +str('-')+str(6+7*(zing-1)) + ' days'
        
        return week
    
    def  __cohort(self,date1,date2):
        '''
        This method will generate the cohorts between the given date ranges
        '''
        range_list = []
        while date1 > date2:
            range2 = date1 - datetime.timedelta(6)
            range_list.append((range2,date1))
            date1 = range2 -datetime.timedelta(1)
        return range_list
    
    def open_result_in_browser(self,html_file_name,html_document):
        '''
        This method will open the report in browser
        '''
        html_file =  html_file_name + '.html'
        f = open(html_file,'w')
        f.write(html_document)
        f.close()
        webbrowser.open_new_tab(html_file)

    
    def work_with_data(self):
        '''
        This method will work with data such as reading data,
        data cleansing and aggregation
        '''
        customers =  pd.read_csv(self.customer_file_path)
        orders    =  pd.read_csv(self.order_file_path)
        
        #Changing the data type to string and striping the spaces
        customers['id'] = customers['id'].astype(str)
        customers['id'] = customers['id'].str.strip()
        orders['user_id'] = orders['user_id'].astype(str)
        orders['user_id'] = orders['user_id'].str.strip()
        
        #Making Joins on Customer and Orders
        df = pd.merge(customers,orders , how = 'left',left_on = 'id',right_on = 'user_id')
        df = df[['id_x','created_x','order_number','created_y']]
        df.rename(columns = {'id_x':'user_id','created_x':'signup_date','created_y':'order_date'},inplace = True)
        df.sort_values(['signup_date'], ascending = False).head()
        #Changing the datatype to datetime
        
        df['signup_date'] = pd.to_datetime(df['signup_date'])
        df['order_date'] = pd.to_datetime(df['order_date'])
		#The time zone configuration can be done here
        #df['signup_date']=df['signup_date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Hong_Kong')
        #df['order_date']=df['order_date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Hong_Kong')
        
        #Removing the timestamp to keep the date only in yyyy-mm-dd format
        df['signup_date1'] = pd.DatetimeIndex(df.signup_date).normalize()
        df['order_date1'] = pd.DatetimeIndex(df.order_date).normalize()
        df = df[['user_id','order_number','signup_date1','order_date1']]
        #Calculating the week range based on signup and order date
        df['diff'] = df['order_date1'].sub(df['signup_date1'], fill_value=0)
        df.sort_values(['signup_date1'],ascending = False,inplace = True)
        #Change the data type to int from timedelta
        df['diff'] =  df['diff'].dt.days
        df['date_range'] = df['diff'].apply(lambda x :self.__calculate_week_range(x) )
        #Getting the first and last date to generate the cohorts
        date1 = df[:1]['signup_date1'].tolist()[0]
        date2 = df[-2:-1]['signup_date1'].tolist()[0]
        range_date = self.__cohort(date1,date2)
        #Generating the actual cohort 
        df['Cohort'] = df['signup_date1'].apply(lambda x : [y[0].strftime('%Y/%m/%d')+'-' + y[1].strftime('%Y/%m/%d') for y in range_date if y[0]<=x<=y[1]][0])
        df.fillna('No Order',inplace = True)
        df.drop_duplicates(['user_id','date_range'],inplace = True)
        df['total_order'] = df['order_number'].apply(lambda x :  1 if not x =='No Order' else 0  )
        #Logic to count only distinct users
        all_user_id = df['user_id'].tolist()
        
        def drop_duplicate_users(x):
            '''
            This method will keep track of users and set 0 for the second or futher occurance of users
            '''
            users_processed = []
            if x in users_processed:
                return 0
            else:
                users_processed.append(x)
                return 1
        df['users'] = df['user_id'].apply(lambda x :  drop_duplicate_users(x) )
        
        #Creating a new field to indetify fisrt time orders
        df['new'] = np.where(df['order_number'] == 1.0,1,0)
        df = df[['users','Cohort','date_range','new','total_order']]
        df.reset_index(inplace = True,drop = True)
        df2 = df.groupby(['Cohort','date_range']).sum().reset_index()
        # Creating another user dataframe to calculate the comulative customers per cohort
        df_user = df2.groupby(['Cohort'])['users'].sum().reset_index()
        df_user.rename(columns = {'users':'Customers'},inplace = True)
        #Dropping the duplicate users
        #df_user.drop_duplicates('Customers',inplace = True)
        df_user.sort_values('Cohort',inplace = True,ascending = True)
        df_user['Customers'] = df_user['Customers'].cumsum()
        #Joining the users dataframe with comulative sum
        df2 = pd.merge(df2,df_user,how ='left',on ='Cohort')
        #Elminating the data_range 0 ie. no orders
        df2 = df2[df2['date_range'] != '0']
        df2.sort_values(['Cohort','date_range'],ascending = False,inplace = True)
        df2 = df2[['Cohort','date_range','Customers','total_order','new']]
        df2['total_order1'] = df2['total_order']/df2['Customers']*100
        df2['new1'] = df2['new']/df2['Customers']*100
        df2 = df2.round(3)
        df2['total_order1'] = df2['total_order1'].apply(lambda x : str(x)+'% orderers')
        df2['new1'] = df2['new1'].apply(lambda x : str(x)+'% 1st time')
        df2['total_order'] = df2['total_order'].apply(lambda x : ' ('+ str(x) + ')\n')
        df2['new'] = df2['new'].apply(lambda x : ' ('+ str(x) + ')')
        df2['total_order1'] = df2['total_order1'] + df2['total_order']
        df2['new1'] = df2['new1'] + df2['new']
        df2['final_column']  =df2['total_order1']+df2['new1']
        df2 = df2[['Cohort','Customers','date_range','final_column']]
        df3 = df2[['Cohort','Customers']]
        df2 = df2.pivot(index='Cohort', columns='date_range', values='final_column')
        df2.columns.name = None
        col = ['0-6 days','7-13 days', '14-20 days', '21-27 days', '28-34 days', '35-41 days', '42-48 days', '49-55 days', '56-62 days','63-69 days' ]
        df2 = df2[col]
        df2 = df2.reset_index()
        df4 = pd.merge(df3,df2 ,how = 'left',on = 'Cohort')
        df4.fillna('',inplace = True)
        df4.drop_duplicates(['Cohort','Customers'],inplace = True)
        df4['Customers'] = df4['Customers'].apply(lambda x : str(x) +' customers')
        df_html = df4.to_html().replace("\\n","<br>")
		
        #ToDo - Below will work only with window.Need to implement for non-window os
        self.open_result_in_browser('cohort_analysis',df_html)
        
        



if __name__ == '__main__':
    customer_file =  r"C:\Users\M71846\Desktop\customers.csv"
    order_file    =  r"C:\Users\M71846\Desktop\orders.csv"
    cohort = Cohort(customer_file,order_file,"Asia/Hong_Kong")
    cohort.work_with_data()






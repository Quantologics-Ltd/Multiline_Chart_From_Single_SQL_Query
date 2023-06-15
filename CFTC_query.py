# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import matplotlib as mpl
import pandasql as ps
import sqlite3
import random as rn

mpl.interactive(False)

##############################################################

def simulate_commodity_managed_money(start_date, end_date, start_value, mu, st_dev, inflation_rate, commodity_name):
    dates = pd.date_range(start_date, end_date, freq='7D')
    df = pd.DataFrame({commodity_name: [], 'date': []})
    val_i = start_value
    for i in range(0, len(dates)):
        delta = rn.gauss(mu, st_dev)*(1+inflation_rate/52)
        print(delta)
        val_i = val_i + delta
        df.loc[i, commodity_name] = abs(val_i)
        df.loc[i, 'date'] = dates[i]
    return df


def simulate_long_short_positions(start_date, end_date, start_value, mu, st_dev, inflation_rate, commodity_name):
    dates = pd.date_range(start_date, end_date, freq='7D')
    df = pd.DataFrame({'long_positions': [], 'short_positions': [], 'date': []})
    val_i = start_value
    for i in range(0, len(dates)):
        delta = rn.gauss(0, 5)*(1+inflation_rate/365)
        print(delta)
        val_i = val_i + delta
        val_i = round(val_i)
        df.loc[i, 'long_positions'] = abs(val_i)
        df.loc[i, 'date'] = dates[i]
    val_i = start_value
    for i in range(0, len(dates)):
        delta = rn.gauss(0, 5)
        print(delta)
        val_i = val_i + delta
        val_i = round(val_i)
        df.loc[i, 'short_positions'] = -abs(val_i)
        df.loc[i, 'date'] = dates[i]
    return df


def plot_commodity_managed_money(df, commodity_name):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df.date, df[commodity_name])
    ax.set(xlabel='Date', ylabel='Value', title= commodity_name)
    #ax.xaxis.set_major_formatter(plt.FixedFormatter(df.index.strftime('%b %d, %Y')))
    plt.xticks(rotation=45)
    plt.show()
    
    
def plot_long_short_positions(df, commodity_name):
    y1 =  df['long_positions']
    y2 =  df['short_positions']
    x = df['date']
    plt.xticks(rotation=45)
    plt.plot(x, y1, label='long')
    plt.plot(x, y2, label='short')
    plt.axhline(0, color='black', linestyle='-')
    # Add axis labels and a title
    plt.title('Long & Short Positions - '+str(commodity_name))
    # Add a legend
    plt.legend()
    plt.show()

###############################################################################

def view_columns(cursor):
    column_names = [column[0] for column in cursor.description]
    for column in column_names:
        print(column)

def save_query_to_df(cursor):
    result = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    return df

def prepare_data(df, column):
    q2023 = df[df['year'] == '2023']
    q2022 = df[df['year'] == '2022']
    t2023 = q2023.week_number
    t2022 = q2022.week_number
    red = q2023[column]
    yellow = q2022[column]
    grey_up = q2022['max_' + column]
    grey_down = q2022['min_' + column]
    orange = q2022['avg_' + column]
    data = [grey_down, grey_up, red, yellow, orange, t2022, t2023]
    return data

def plot_final(data, column):
    grey_down, grey_up, red, yellow, orange, t2022, t2023 = data
    fig, ax = plt.subplots()
    ax.set_title(column+ ' positions', pad=30)
    ax.fill_between(t2022, grey_down, grey_up, alpha=.5, linewidth=3, color='grey', label=['5Y Max', '5Y Min'])
    ax.plot(t2022, orange  ,linewidth=3, color='orange', linestyle='dashed', label='5Y Avg')
    ax.plot(t2022, yellow  ,linewidth=3, color='yellow', label='2022')
    ax.plot(t2023, red  ,linewidth=3, color='red', label='2023')
    ax.set_xlabel('Week number')
    ax.set_xlim(1, 52)
    fig.tight_layout()
    fig.legend(loc='outside upper right')
    plt.show()



if __name__ == "__main__":

    # simulate example data
    start_date = datetime(2012, 1, 3)
    end_date = datetime(2023, 6, 13)
    df = simulate_commodity_managed_money(start_date, end_date, 50, 0.04, 2, 0.02, 'managed_money')
    plot_commodity_managed_money(df, 'managed_money')
    df2 = simulate_long_short_positions(start_date, end_date, 0, 0, 100, 0.02, 'example_commodity')
    plot_long_short_positions(df2, 'example_commodity')
    df3 = ps.sqldf(""" select df.managed_money, df.date, df2.long_positions, df2.short_positions
                        from df join df2 on df.date = df2.date""")

    # insert example data to a database
    conn = sqlite3.connect('cot.db')
    try:
        schema = '''
        drop table cot;
        '''
        conn.execute(schema)
    except:
        pass
    schema = '''
        CREATE TABLE cot (
            managed_money REAL,
            date TEXT NOT NULL,
            price REAL,
            long_positions INTEGER,
            short_positions INTEGER
        )
    '''
    conn.execute(schema)

    df3.to_sql('cot', conn, if_exists='replace', index=False)

    query = """ 
    select t4.* , t3.max_ratio, t3.min_ratio, t3.avg_ratio, strftime('%Y', date) as year, t3.avg_net, t3.max_net, t3.min_net, avg_managed_money, max_managed_money, min_managed_money
    from 
        (
        select t2.week_number,  max(t2.ratio) as max_ratio , min(t2.ratio)  as min_ratio, sum(t2.ratio)/count(t2.ratio) as avg_ratio, avg(t2.net) as avg_net, max(t2.net) as max_net, min(t2.net) as min_net,
        max(managed_money) as max_managed_money, avg(managed_money) as avg_managed_money, min(managed_money) as min_managed_money
        from (
                select managed_money, date, long_positions, 
                short_positions, long_positions-short_positions as net,
                case when short_positions = '0' then NULL 
                    else  long_positions/abs(short_positions) end as ratio,
                     case when strftime('%j', date) % 7 = 1 then strftime('%j', date)/7
                            when strftime('%j', date) % 7 = 0 then strftime('%j', date)/7
                            when strftime('%j', date) % 7 > 1 then strftime('%j', date)/7+1
                            end as week_number
                from (   select managed_money, date, long_positions, 
                         short_positions, long_positions-short_positions as net,
                         case when short_positions = '0' then NULL 
                         else  long_positions/abs(short_positions) end as ratio,
                         case when strftime('%j', date) % 7 = 1 then strftime('%j', date)/7
                                when strftime('%j', date) % 7 = 0 then strftime('%j', date)/7
                                when strftime('%j', date) % 7 > 1 then strftime('%j', date)/7+1
                                end as week_number
                        FROM cot
                        where strftime('%Y%m%d', date) > strftime('%Y', 'now', '-5 years') ) as  t 
                        group by managed_money, date, long_positions, 
                                short_positions, long_positions-short_positions,
                                case when short_positions = '0' then NULL 
                                    else  long_positions/short_positions end,
                                     case when strftime('%j', date) % 7 = 1 then strftime('%j', date)/7
                                            when strftime('%j', date) % 7 = 0 then strftime('%j', date)/7
                                            when strftime('%j', date) % 7 > 1 then strftime('%j', date)/7+1
                                            end ) as t2
                                            where strftime('%Y', t2.date) != '2023'
    	                                    group by  t2.week_number 
    	                                    	 ) 
    	                                        as  t3  join 
    	                                         (
    	                                    select managed_money, date, long_positions, 
                                                 short_positions, long_positions-short_positions as net,
                                                 case when short_positions = '0' then NULL 
                                                 else  long_positions/abs(short_positions) end as ratio,
                                                 case when strftime('%j', date) % 7 = 1 then strftime('%j', date)/7
                                                        when strftime('%j', date) % 7 = 0 then strftime('%j', date)/7
                                                        when strftime('%j', date) % 7 > 1 then strftime('%j', date)/7+1
                                                        end as week_number
                                            from (
                                                 select managed_money, date, long_positions, 
                                                 short_positions, long_positions-short_positions as net,
                                                 case when short_positions = '0' then NULL 
                                                 else  long_positions/abs(short_positions) end as ratio,
                                                 case when strftime('%j', date) % 7 = 1 then strftime('%j', date)/7
                                                        when strftime('%j', date) % 7 = 0 then strftime('%j', date)/7
                                                        when strftime('%j', date) % 7 > 1 then strftime('%j', date)/7+1
                                                        end as week_number
                                                 from cot 
                                                 where strftime('%Y%m%d', date) > strftime('%Y', 'now', '-5 years') ) as  t 
                                                 group by managed_money, date, long_positions, 
                                                 short_positions, long_positions-short_positions,
                                                 case when short_positions = '0' then NULL 
                                                      else  long_positions/short_positions end,
                                                      case when strftime('%j', date) % 7 = 1 then strftime('%j', date)/7
                                                            when strftime('%j', date) % 7 = 0 then strftime('%j', date)/7
                                                            when strftime('%j', date) % 7 > 1 then strftime('%j', date)/7+1
                                                            end
                                                         ) as t4
                                                on t4.week_number =  t3.week_number 
                                                order by date                                       
                                            """

    cursor = conn.execute(query)
    view_columns(cursor)
    df = save_query_to_df(cursor)


    data = prepare_data(df, 'net')
    plot_final(data, 'net')

    data = prepare_data(df, 'ratio')
    plot_final(data, 'ratio')

    data = prepare_data(df, 'managed_money')
    plot_final(data, 'managed_money')







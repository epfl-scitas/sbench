import sqlite3
import argparse
import json
import seaborn as sbn
import matplotlib.pyplot as plt
import pandas as pd
import math
import numpy as np


def parse_input_args():
    """Parse the command line arguments:
    Return: 
       sql_query: The dictionary containing the SQL query
       plot_params: The dictionary containing parameters for plotting
       input_args.benchmark_db: The file containing the benchmark db
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("benchmark_db")
    parser.add_argument("json_query")
    input_args = parser.parse_args()

    with open(input_args.json_query) as json_file:
        inputs = json.load(json_file)
    sql_query = inputs[0]
    plot_params = inputs[1]

    return sql_query, plot_params, input_args.benchmark_db

def build_sql_query(sql_query):
    """Build the sql_query to extract data from db.
    Args:
        sql_query: the dictionary with the SQL query
    Return:
        sql_query: the SQL query build from sql_query.json
        df: the pandas dataframe
    """
     
    df = pd.DataFrame(columns=sql_query['select'].split(','))
    query = "select "+sql_query['select']+" from "+sql_query['from']\
                +" "+sql_query['join']
    if 'where' in sql_query:
        query += " where "+sql_query['where']
    if 'order' in sql_query:
        query += " order by "+sql_query['order']
    return query, df

def db_extract(sql_query, db, df, col):
    """Extract the query specified by the string, and sort the dataframe.
    Args:
        sql_query: the SQL query build from sql_query.json
        col: columns to sort the dataframe
        db: the benchmark database
    Return:
        df: the dataframe sorted by columns
    """
    db = sqlite3.connect(db)
    db_cursor = db.cursor()
    for row in db_cursor.execute(sql_query):
        df = df.append(pd.Series(row,index=df.columns),ignore_index=True)
    df = df.sort_values(by=col)
    db.close()
    return df


def change_width(ax, new_value) :
    """Adjust the thickness of the bars
    """
    for patch in ax.patches :
        current_width = patch.get_width()
        diff = current_width - new_value

        # we change the bar width
        patch.set_width(new_value)

        # we recenter the bar
        patch.set_x(patch.get_x() + diff * .5)

def plot_data(data, plot_params, type='seaborn'):
    """Plot the data.
    Args:
        data: the pandas dataframe to be plotted
        plot_params: the dictionary containing the plotting parameters
        type: the type of graph to be plotted
    """
    if type == 'seaborn':
        my_dpi = plot_params['dpi']
        fig = plt.figure(figsize=(plot_params['res'][0]/my_dpi,plot_params['res'][1]/my_dpi), dpi=my_dpi)
        plot = sbn.barplot(x=plot_params['x'], y=plot_params['y'], hue=plot_params['hue'],\
                           data=data)
        plot.set_yticklabels(plot.get_yticks(),fontsize=plot_params['fontsize'])
        plot.set_xticklabels(plot.get_xticklabels(),rotation=90,fontsize=plot_params['fontsize'])
        plot.legend(loc=0,borderaxespad=0.,fontsize=plot_params['fontsize'])
        plt.title(plot_params['title'])
        change_width(plot, 0.55)
        i = 0
        for p in plot.patches:
            height = p.get_height()
            if math.isnan(height) == False:
                plot.text(p.get_x()+p.get_width()/2.,height,
                    data.iloc[i][plot_params['bartext']],ha="left",rotation=90,
                    rotation_mode="anchor",fontsize=plot_params['fontsize']) 
                i += 1
        fig.tight_layout()
        plot.set_ybound(np.array(plot.get_ybound())*1.10)
        plt.savefig(plot_params['output_file'])
    else:
        print('Please provide a plotting method. Types: seaborn')

if __name__ == '__main__':
    sql_query, plot_params, db = parse_input_args()
    sql_query, df = build_sql_query(sql_query)
    data = db_extract(sql_query, db, df, plot_params['sort_df'])
    plot_data(data,plot_params)  


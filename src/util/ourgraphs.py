#########################################################################
#
# ourgraphs.py
#
# @author: Phil Mui
# @email: thephilmui@gmail.com
# @date: Mon Jan 23 16:45:56 PST 2023
#
#########################################################################

import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import linregress

plt.rcParams['figure.dpi'] = 140

def showTopicsResults(results_df, title, columns):

    # Create a figure with subplots
    fig, axs = plt.subplots(nrows=len(columns), ncols=1, sharex=True, 
                            figsize=(8,len(columns)*4))

    # Loop through the columns and plot each one
    for ax, column in zip(axs, columns):
        # Select the data for the current column
        x = range(len(results_df.index))
        y = results_df[column]
        
        # Calculate the linear regression
        slope, intercept, r_value, p_value, std_err = linregress(x, y)
        
        print("{} slope:{:<.5f}, int:{:<.5f}, r:{:<.5f}, p-value:{:<.5f}, se:{:<.5f}, x:{}".format(
                column, slope, intercept, r_value, p_value, std_err, x))

        # Plot the data and the linear regression line
        results_df[column].plot(ax=ax, style=".", x=x, y=y, label=column)
        ax.plot(x, intercept + slope*x, 'b:', label='regression')
        ax.set_ylim(0, max(y)*1.2)
        
        # Add a legend
        ax.legend(loc='lower center')

        # Add x-label for the years
        ax.set_title(column + ' slope: ' + "{:.2e}".format(slope))
        ax.set_xticks(results_df.index)
        ax.set_xticklabels(results_df.year)
        
    plt.suptitle(title, fontsize=18)
    return plt

def showKeyTopicsResults(results_df, title, columns):
    # Create a figure with subplots
    fig, axs = plt.subplots(nrows=len(columns), ncols=1, sharex=True, 
                            figsize=(8,3*len(columns)))

    # Loop through the columns and plot each one
    for ax, column in zip(axs, columns):
        # Select the data for the current column
        x = range(len(results_df.index))
        y = results_df[column]

        # Calculate the linear regression
        slope, intercept, r_value, p_value, std_err = linregress(x, y)

        print("{} slope:{:.5f}, int:{:.5f}, r:{:.5f}, p:{:.5f}, se:{:.5f}, x:{}".format(
                column, slope, intercept, r_value, p_value, std_err, x))

        # Plot the data and the linear regression line
        results_df[column].plot(ax=ax, style=".", x=x, y=y, label=column)
        ax.plot(x, intercept + slope*x, 'b:', label='regression')
        ax.set_ylim(0, max(y)*1.2)

        # Add a legend
        ax.set_title(column + ' slope: ' + "{:.1e}".format(slope), fontsize=10)
        ax.legend(loc='lower center')

        # Add x-label for the years
        ax.set_xticks(results_df.index)
        ax.set_xticklabels(results_df.year)

    plt.suptitle(title, fontsize=18)
    return plt
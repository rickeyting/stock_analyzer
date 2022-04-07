import pandas as pd
import numpy as np


'''
Western Electric Rule
One point above UCL or below LCL
Two points above/below 2 sigma
Four out of five points above/below 1 sigma
Eight points in a row above/below the center line
'''

def western_electric_test(df, col, term=30, sort_lable='date'):
    df = df.sort_values(by=sort_lable, ascending=False).reset_index(drop=True)
    for i in range(len(df)-(term-1)):
        inspection = df.loc[[i:i+term],col].reset_index(drop=True)
        mean = round(inspection[col].mean(),2)
        sigma = round(inspection[col].std(),2)

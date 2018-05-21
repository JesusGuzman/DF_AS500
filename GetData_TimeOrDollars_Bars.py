import pandas as pd
import numpy as np
from datetime import datetime
from os import listdir, mkdir, path

'''*********************************************************
**********************GET DOLLAR BARS***********************
*********************************************************'''
#Funcion principal para obtener los dollar bars
def GetDollarBars(first_date, last_date, ticker_list, total_volume,
                  columns= ['OpenPrice','HighPrice','LowPrice','ClosePrice'], 
                  folder="./emisoras/Dollar_Bars/"):
 
    if not (path.isdir(folder)):
        mkdir( folder );
    for ticker in ticker_list:
        df = csv_to_df(ticker)
        df = limit_df(first_date, last_date, df)
        df = get_df_dollar_bars(df,total_volume)
        df = get_columns(df,columns)
        df.to_csv(folder+ticker, encoding='utf-8', index=False)
    return df

def get_df_dollar_bars(df,total_volume):
    bars = []
    volume = 0
    first_index = 0
    for row in df.itertuples():
        volume = volume + row[9]
        if volume >= total_volume:
            nf = df[first_index:row.Index+1]
            stock = { 'OpenPrice': nf[nf.columns[4]][first_index], 'HighPrice': nf[nf.columns[5]].max(), 
                      'LowPrice': nf[nf.columns[6]].min(),   'ClosePrice': nf[nf.columns[7]][row.Index] }
            bars.append(stock)
            first_index=row.Index+1
            
            volume = 0
            
    df_stocks = pd.DataFrame(bars,columns=['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice'])        
                 
    return df_stocks

'''********************************************************
************************TIME BARS**************************
********************************************************'''

#FUNCION PRINCIPAL PARA OBTENER LOS TIME BARS
def GetTimeBars(first_date, last_date, ticker_list,
                period, columns= ['Date',          'Timestamp',      'Ticker',      'OpenPrice',
                                  'HighPrice',     'LowPrice',       'ClosePrice',  'TotalVolume',
                                  'TotalQuantity', 'TotalTradeCount'],
                folder="./emisoras/Time_Bars/"):

    if not (path.isdir(folder)):
        mkdir( folder );
    if type(period) == str :#period.contains("d"):
        period = int(period.replace('d', ''))
        for ticker in ticker_list:
            df_final = pd.DataFrame(columns= columns)
            df = csv_to_df(ticker)
            df = limit_df(first_date, last_date, df)
            df = change_period_days(period,df)
            df = get_columns(df,columns)
            df_final = df_final.append(df,ignore_index=True)
            df_final.to_csv(folder+ticker, encoding='utf-8', index=False)
    
    else:
        for ticker in ticker_list:
            df_final = pd.DataFrame(columns= columns)
        
            df = csv_to_df(ticker)
            df = limit_df(first_date, last_date, df)
            for day in get_days(df):
                ndf = df_day(day,df)    
                ndf.drop(ndf.columns[0], axis=1, inplace=True)
                ndf = change_period(period, ndf)
                ndf = get_columns(ndf,columns)
                df_final = df_final.append(ndf,ignore_index=True)
                df_final.to_csv(folder+ticker, encoding='utf-8', index=False)
    return df_final

#Obtenemos los dias que hay en el DF
def get_days(df):
    df_days = df[df.columns[1]]
    days = pd.unique(pd.Series(df_days))
    return days

#Obtenemos un DF del dia indicado
def df_day(day,df):
    date = df[df.columns[1]]
    df = df[date==day]
    return df.reset_index()

#Cambiamos el periodo del DF
def change_period(new_time, df):
    tam = new_time
    i=0
    stocks = []
    first = 0
    while i < (len(df)//tam):
        nf = df[first:new_time]
        stock = { 'Date':   nf[nf.columns[1]][first],          'Timestamp': nf[nf.columns[2]][first],
                  'Ticker': nf[nf.columns[3]][first],          'OpenPrice': nf[nf.columns[4]][first],
                  'HighPrice': nf[nf.columns[5]].max(),        'LowPrice': nf[nf.columns[6]].min(),
                  'ClosePrice': nf[nf.columns[7]][new_time-1], 'TotalVolume': nf[nf.columns[8]].sum(),
                  'TotalQuantity': nf[nf.columns[9]].sum(),    'TotalTradeCount': nf[nf.columns[10]].sum() }
        stocks.append(stock)

        first = first + tam
        new_time = new_time + tam
        i = i + 1

    if (len(df)%tam) != 0:
        first = len(df)-(len(df)%tam)
        new_time = len(df)
        nf = df[first:new_time]
        stock = { 'Date':   nf[nf.columns[1]][first],          'Timestamp': nf[nf.columns[2]][first],
                  'Ticker': nf[nf.columns[3]][first],          'OpenPrice': nf[nf.columns[4]][first],
                  'HighPrice': nf[nf.columns[5]].max(),        'LowPrice': nf[nf.columns[6]].min(),
                  'ClosePrice': nf[nf.columns[7]][new_time-1], 'TotalVolume': nf[nf.columns[8]].sum(),
                  'TotalQuantity': nf[nf.columns[9]].sum(),    'TotalTradeCount': nf[nf.columns[10]].sum() }
        stocks.append(stock)
    df_stocks = pd.DataFrame(stocks,columns=['Date',          'Timestamp',     'Ticker',     'OpenPrice',
                                             'HighPrice',     'LowPrice',      'ClosePrice', 'TotalVolume',
                                             'TotalQuantity', 'TotalTradeCount'])
    return df_stocks

#Funcion que cambia el periodo por dias 
def change_period_days(period,df):
    tam=period
    days = get_days(df)
    i=0
    key = 0
    stocks = []
    date = df[df.columns[1]]
    while key < (len(days)//tam):
        nf= df[(date>=days[i])&(date<=days[(i+tam)-1])]
        
        array = nf.index
        init = int(array[0])
        final = int(array[len(array)-1])
        stock = { 'Date': nf['Date'][init] ,          'Timestamp': '09:30:00',
                 'Ticker': nf['Ticker'][init],          'OpenPrice': nf[nf.columns[4]][init],
                 'HighPrice': nf['HighPrice'].max(),        'LowPrice': nf['LowPrice'].min(),
                 'ClosePrice': nf['ClosePrice'][final], 'TotalVolume': nf['TotalVolume'].sum(),
                 'TotalQuantity': nf['TotalQuantity'].sum(),    'TotalTradeCount': nf['TotalTradeCount'].sum() }
        stocks.append(stock)
        i = i+tam
        key = key +1
        
    if (len(days)%tam) != 0:
        nf = df[(date>=days[len(days)-((len(days)%tam))])]
        array = nf.index
        init = int(array[0])
        final = int(array[len(array)-1])
        stock = { 'Date': nf['Date'][init] ,          'Timestamp': '09:30:00',
                 'Ticker': nf['Ticker'][init],          'OpenPrice': nf[nf.columns[4]][init],
                 'HighPrice': nf['HighPrice'].max(),        'LowPrice': nf['LowPrice'].min(),
                 'ClosePrice': nf['ClosePrice'][final], 'TotalVolume': nf['TotalVolume'].sum(),
                 'TotalQuantity': nf['TotalQuantity'].sum(),    'TotalTradeCount': nf['TotalTradeCount'].sum() }
        stocks.append(stock)

    df_stocks = pd.DataFrame(stocks,columns=['Date',          'Timestamp',     'Ticker',     'OpenPrice',
                                             'HighPrice',     'LowPrice',      'ClosePrice', 'TotalVolume',
                                             'TotalQuantity', 'TotalTradeCount'])
    return df_stocks

'''********************************************************
********FUNCIONES QUE USAN DOLLAR BARS Y TIME BARS*********
********************************************************'''
#Funcion que limita un DF en un Rango de fechas indicado
def limit_df(first_date, last_date, df):
    date = df[df.columns[0]]
    df = df[(date>=first_date)&(date<=last_date)]
    return df.reset_index()

#Funcion que crea un nuevo DF con as columnas indicadas
def get_columns(df,columns):
    nf = pd.DataFrame()
    for column in columns:
        nf[column] = df[column]
    return nf

#Crear un DF de una emisora apartir de un CSV
def csv_to_df(ticker):
    file = pd.read_csv('./emisoras/All-Complete-Ticker-Data/'+ticker)
    df = pd.DataFrame(file)
    return df



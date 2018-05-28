import pandas as pd
from os import listdir
from datetime import datetime, timedelta

#obtener los nombres de los tickers
#dentro del csv maestro
def get_tickers(df):
    df_tickers = df[df.columns[2]]
    tickers = pd.unique(pd.Series(df_tickers))
    return tickers

#crear los distintos CSV’s por ticker
def create_csv_ticker(df, tickers):
    df_tickers = df[df.columns[2]]
    for ticker in tickers:
        new_df = df[df_tickers == ticker].sort_values(by=['Date','Timestamp'], ascending=True)
        new_df.to_csv("./emisoras/All-Data-by-Ticker/" +ticker.replace('/','-')+'.csv', encoding='utf-8', index=False)

#obtener el nombre de los archivos
def ls(pwd):
    return listdir(pwd)

#############CREACION CSV POR AÑO########################
def CreateFileMaster():
    csvs = ls('./AS500-all') #Obtenemos todos los archivos que hay en año
    csv = pd.concat( [ pd.read_csv("./AS500-all/"+csv) for csv in csvs] ) #Concatenamos todos los archivos en un DF
    csv.to_csv( "AS500-master.csv", index=False ) #Guardamos el DF en un csv con todos los datos del año

#############CREACION DE CSV’s POR EMISORA############## 
def CreateTickers():
    file = pd.read_csv('AS500-master.csv') #indicamos el CSV que vamos a particionar
    df = pd.DataFrame(file) #creamos el DF con ese CSV
    tickers = get_tickers(df) #Obtenemos una lista de las emisoras que se encuentran en el DF
    create_csv_ticker(df,tickers) #Apartir de la lista de emisoras se crean los distintos CSV’s +500

''' **********************************************************************************************************
**************************************************************************************************************
************************************************************************************************************'''

#Parsear String a timestamp
def parse_time(x):
    datetime_object = datetime.strptime(x,'%H:%M:%S')
    return datetime_object 

#Obtener los indices y el numero de columnas faltantes
def get_index(df):
    news_rows = []
    for row in df.itertuples(index=True):
        index = row.Index
        date_1 = parse_time(df['Timestamp'][index])
        if index < (len(df)-1):
            date_2 =  parse_time( df['Timestamp'][index+1])
        else:
            date_2 =  parse_time( df['Timestamp'][index])
        minutes = ((date_2 - date_1).seconds)/60
        if (minutes > 1)&(minutes< 1049) :
            news_rows.append({'index': index, 'num': minutes-1})
    return news_rows

#==========================================================================
def create_df_complete(df, news_rows): #agregamos la primer barra
    indexs = []
    num_rows = []
    
    for key in news_rows:
        indexs.append(key['index'])
        num_rows.append(key['num'])
        
    
    
    df_final = pd.DataFrame(columns=['Date',          'Timestamp', 'Ticker',     'OpenPrice',
                                     'HighPrice',     'LowPrice',  'ClosePrice', 'TotalVolume', 
                                     'TotalQuantity', 'TotalTradeCount'])
    
    

    key = 0
    ind = 0
    while key <= len(df):
        if ind == len(num_rows):
            ndf = df[key:]
            df_final = df_final.append(ndf,ignore_index=True)
            break
        else:
            ndf = df[key:indexs[ind]+1]
            conta = 0
            array = []
            while conta<num_rows[ind]:
            
                stock = { 'Date':   df[df.columns[0]][indexs[ind]], 
                          'Timestamp': (parse_time(df[df.columns[1]][indexs[ind]]) + timedelta(minutes=conta+1)).strftime('%H:%M:%S'),
                          'Ticker': ndf[ndf.columns[2]][indexs[ind]],     'OpenPrice': ndf[ndf.columns[3]][indexs[ind]], 
                          'HighPrice': ndf[ndf.columns[4]][indexs[ind]],  'LowPrice': ndf[ndf.columns[5]][indexs[ind]],
                          'ClosePrice': ndf[ndf.columns[6]][indexs[ind]], 'TotalVolume': 0,
                          'TotalQuantity': 0,                             'TotalTradeCount': 0 }
                
                array.append(stock)
                conta = conta + 1
                
            df_stock = pd.DataFrame(array)
                
            df_final = df_final.append( [ndf,df_stock] ,ignore_index=True)
                
            key = indexs[ind]+1 #key + indexs[ind]+1
            ind = ind + 1
            
        
    
    df_final = df_final[['Date',          'Timestamp', 'Ticker',     'OpenPrice',
                             'HighPrice',     'LowPrice',  'ClosePrice', 'TotalVolume', 
                             'TotalQuantity', 'TotalTradeCount']]
    #df_final = df_final.append( [ndf,df_stock] ,ignore_index=True)
    #df_final.to_csv('./emisoras/All-Complete-Ticker-Data/test.csv', encoding='utf-8', index=False)
    return df_final

#==========================================================================
#Funcion que agrega las filas faltante y crea un nuevo DF
def create_df(df, news_rows):
    indexs = []
    num_rows = []
    for key in news_rows:
        if key['num'] < 1049:
            indexs.append(key['index'])
            num_rows.append(key['num'])
        else:
            continue

    nf = pd.DataFrame(columns=['Date', 'Timestamp', 'Ticker', 'OpenPrice',
                               'HighPrice', 'LowPrice', 'ClosePrice', 'TotalVolume', 
                               'TotalQuantity', 'TotalTradeCount'])
    index = 0
    for row in df.itertuples(index=True):
        lista= [row.Date, row.Timestamp, row.Ticker,
                row.OpenPrice, row.HighPrice, row.LowPrice, row.ClosePrice,
                row.TotalVolume, row.TotalQuantity, row.TotalTradeCount]
    
        nf.loc[index] = lista
    
        if row.Index in indexs:
            index_array = indexs.index(row.Index)
            index = index + 1
            key = 0
            while key < num_rows[index_array]:
                nf.loc[index] = [row.Date, (parse_time(row.Timestamp) + timedelta(minutes=key+1)).strftime('%H:%M:%S'),
                                 row.Ticker,row.OpenPrice, row.HighPrice, row.LowPrice, row.ClosePrice, 0, 0, 0]
                key = key + 1
                index = index + 1
            
        index = index + 1
    return nf


def complete_data(tickers):
    for ticker in tickers:
        file = pd.read_csv('./emisoras/All-Clean-Ticker-Data/'+ticker)
        df = pd.DataFrame(file)    
        index = get_index(df)
        if len(index)>0:
            new_df = create_df_complete(df, index)
            new_df.to_csv('./emisoras/All-Complete-Ticker-Data/'+ticker, encoding='utf-8', index=False)
        else:
            df.to_csv('./emisoras/All-Complete-Ticker-Data/'+ticker, encoding='utf-8', index=False)
            
def clean_data(tickers):
    first = []
    for ticker in tickers:
        file = pd.read_csv('./emisoras/All-Data-by-Ticker/'+ticker)
        df = pd.DataFrame(file)
        date = df[df.columns[1]]
        new_df = df[(date>='09:30:00')&(date<'16:01:00')]
        new_df.to_csv('./emisoras/All-Clean-Ticker-Data/'+ticker, encoding='utf-8', index=False)
        file = pd.read_csv('./emisoras/All-Clean-Ticker-Data/'+ticker)
        df_clean = pd.DataFrame(file)
        nf = df_clean[0:1]
        if nf['Timestamp'][0] == '09:30:00':
            continue
        else:    
            stock = { 'Date':   nf[nf.columns[0]][0],          'Timestamp': '09:30:00',
                  'Ticker': nf[nf.columns[2]][0],          'OpenPrice': nf[nf.columns[3]][0], 
                  'HighPrice': nf[nf.columns[4]][0],        'LowPrice': nf[nf.columns[5]].min(),
                  'ClosePrice': nf[nf.columns[6]][0], 'TotalVolume': 0,
                  'TotalQuantity': 0,    'TotalTradeCount': 0 }
        
            df_stocks = pd.DataFrame([stock],columns=['Date',          'Timestamp',     'Ticker',     'OpenPrice',
                                             'HighPrice',     'LowPrice',      'ClosePrice', 'TotalVolume', 
                                             'TotalQuantity', 'TotalTradeCount'])
        
            nf_final = df_stocks.append(new_df,ignore_index=True)
            nf_final.to_csv('./emisoras/All-Clean-Ticker-Data/'+ticker, encoding='utf-8', index=False)
            
def get_init(df):
    rows = []
    date = df['Timestamp']
    df = df[date>='16:00:00']
    for row in df.itertuples(index=True):
        rows.append(row.Index)
    return rows

def add_first_row(df,indexs):
    array=[]
    key = 0
    tam = len(indexs)
    for index in indexs: 
        if (key < tam-1):
            if (df['Timestamp'][index+1] != '09:30:00' ):
                array.append(index)
            key = key + 1
    return array

def add_first_bar(tickers): #agregamos la primer barra
    for ticker in tickers:
        file = pd.read_csv('./emisoras/All-Clean-Ticker-Data/'+ticker)
        df = pd.DataFrame(file)   
        indexs = get_init(df)
        new_index = add_first_row(df,indexs)
        key = 0
        ind = 0
        
        df_final = pd.DataFrame(columns=['Date',          'Timestamp', 'Ticker',     'OpenPrice',
                                         'HighPrice',     'LowPrice',  'ClosePrice', 'TotalVolume', 
                                         'TotalQuantity', 'TotalTradeCount'])
        
        while key <= len(df):
            if ind == len(new_index):
                ndf = df[key:]
                df_final = df_final.append(ndf,ignore_index=True)
                break
            else:
                ndf = df[key:new_index[ind]+1]
                stock = { 'Date':   df[df.columns[0]][new_index[ind]+1],     'Timestamp': '09:30:00',
                          'Ticker': ndf[ndf.columns[2]][new_index[ind]],     'OpenPrice': ndf[ndf.columns[3]][new_index[ind]], 
                          'HighPrice': ndf[ndf.columns[4]][new_index[ind]],  'LowPrice': ndf[ndf.columns[5]][new_index[ind]],
                          'ClosePrice': ndf[ndf.columns[6]][new_index[ind]], 'TotalVolume': 0,
                          'TotalQuantity': 0,                                'TotalTradeCount': 0 }
                
                
                df_stock = pd.DataFrame([stock])
                
                df_final = df_final.append( [ndf,df_stock] ,ignore_index=True)
                
                key = new_index[ind] + 1
                ind = ind + 1 
        df_final = df_final[['Date',          'Timestamp', 'Ticker',     'OpenPrice',
                             'HighPrice',     'LowPrice',  'ClosePrice', 'TotalVolume', 
                             'TotalQuantity', 'TotalTradeCount']]
        df_final.to_csv('./emisoras/All-Clean-Ticker-Data/'+ticker, encoding='utf-8', index=False)

def CreateDataComplete():
    #Limitar rango de operacion del mercado
    tickers = ls('./emisoras/All-Data-by-Ticker/')
    clean_data(tickers)
    #Agregar la primer barra del dia
    tickers = ls('./emisoras/All-Clean-Ticker-Data/')
    add_first_bar(tickers)
    #Completar los minutos faltantes 
    tickers = ls('./emisoras/All-Clean-Ticker-Data/')
    complete_data(tickers)

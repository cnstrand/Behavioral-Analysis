import pandas as pd
import os
os.chdir('//tedfil01/DataDropDEV/PythonPOC')
import pyodbc 
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=TEDSQL050;'
                      'Database=RiskPOC;'
                      'Trusted_Connection=yes;')
from datetime import datetime, date, timedelta

processName = 'NCT Daily Trade Updates SQL Injection'

subProcessName = 'Initializing'

processLog(processName, subProcessName,None)

def sqlToDataFrame(DSN, sql):
    pyodbc.pooling = False
    cnxn = pyodbc.connect("DSN=" + DSN, autocommit = True)
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df

def processLog(processName, subprocessName, errorLog = None):
    if errorLog == None:
        successIndicator = '1'
        errorLog = ''
    else:
        successIndicator = '0'
        errorLog = re.sub('[\"\'\,]', '', 'str(errorLog)
    sql = 'insert into dbo.zProcessLog select '" + processName + "', '" + subprocessName + "', '" + getUser() + "', current_timestamp, " + successIndicator + ",'" + erroLog + "'"
    executeSQL("RiskPOC", sql) 
    print(processName + ", " + subprocessName + ", " + getUser() ", " + successIndicator)

def dataFrameToSQL(DSN, df, tablename, csvpath, convert, encodingformat = None):

    #Dumps dataframe (df) to sql server (DSN) table (tablename) using bulk insert from csv (csvpath)
    #Forces conversion to existing table's column datatypes if convert == True
    #If convert == False, insert will fail if datatypes do not align properly
    df.to_csv(csvpath, index = False, header = False, sep = '~', encoding = encodingformat)
    conversiondictionary = {'int64' : 'int', 'datetime64[ns]' : 'smalldatetime', 'datetime32[ns]' : 'smalldatetime', 'object' : 'text', 'float64' : 'float', 'float32' : 'float', '<M8[ns]' : 'smalldatetime', 'bool' : 'varchar(5)'}
    alltables = sqlToDataFrame(DSN, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = 'RISKPOC'")
    tableexists = False
    for table in alltables['TABLE_NAME']:
        if tablename.lower() == table.lower():
            tableexists = True
    if not tableexists:
        print ('Table does not already exist. Creating new table and inserting data.')
        sql = 'create table ' +  tablename + '('
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)]                    
            else:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)] + ','
        sql = sql + ');'
        executeSQL(DSN, sql)
    else:
        print ('Table already exists. Inserting data into existing table.')
    if convert == False:
        sql = 'create table #' +  tablename + '('
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)]                
            else:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)] + ','
        sql = sql + '); bulk insert #' + tablename + " from '" + csvpath + "' with (FIELDTERMINATOR = '~') insert into " + tablename + ' select '
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + conversiondictionary[str(df[df.columns[column]].dtype)] + ')'            
            else:
                sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + conversiondictionary[str(df[df.columns[column]].dtype)] + '),'
        sql = sql + ' from #' + tablename + '; drop table #' + tablename + ';'
        executeSQL(DSN, sql)       
    else:
        existingdtypes = sqlToDataFrame(DSN, "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tablename + "'")
        sql = 'create table #' +  tablename + '('
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column]
                else:
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + ')'
            else:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column] + ','
                else:
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + '),'        
        sql = sql + '); bulk insert #' + tablename + " from '" + csvpath + "' with (FIELDTERMINATOR = '~') insert into " + tablename + ' select '
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + ')'
                else:
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + '))'
            else:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + '),'
                else:
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + ')),'
        sql = sql + ' from #' + tablename + '; drop table #' + tablename + ';'
        executeSQL(DSN, sql)
        #return sql

try:
    lastDate = '2015-01-01'
    startDate2 = datetime.strptime(lastDate,'%Y-%m-%d')
    startDate2 = datetime.date(lastDate)
    startDate = startDate2 - timedelta(days = 260)
    startDate = startDate.strftime('%Y-%m-%d')

    tenor = pd.DataFrame()
    
# tenor1 is for the division domestic grain
# we want to assign data a binary value of 1 or 2 depending on whether the data is within twelve months of the trade date 

    tenor1 = sqlToDataFrame('SYBIQProd',"""
    select loaddate, poscommodity
    , case when source in ('DFI', 'AGR', 'TWS') and trader not in ('') then left(trader, 10) else location end trader
    , sum(abs(DeltaST))absST, BeyondTwelveMonths, subdivision
    from
    (
    Select loaddate, subdivision, poscommodity, DeltaST, DeltaPUOM, source, location
    , PositionSubLoc, PricePrefUOM, PriceUoMCode, PorS
    , MktPricePUOM, MktRegion, TradeDate, trader, ContMonth
    ,contractnumber, CustomerTypeDescription, Broker
    ,Month12, TradeMonth
    ,case when TradeMonth>Month12 then 1 else 0 end BeyondTwelveMonths
    from
    (
    
    SELECT loaddate, s.subdivisiondescription subdivision, d.divisiondescription division, positioncommoditygroupdescription poscommodity, DeltaQuantityShortTons DeltaST
    , DeltaquantityPreferredunitofmeasure DeltaPUOM, source, L.locationdescription location
    , positionsublocation PositionSubLoc, pricepreferredunitofmeasure PricePrefUOM, priceunitofmeasurecode PriceUoMCode, PurchaseorSale PorS
    , marketpricepreferredunitofmeasure MktPricePUOM, marketregiondescription MktRegion, TradeDate, traderdescription trader, contractmonth ContMonth
    ,contractnumber, CustomerTypeDescription, brokerdescription Broker, PositionType
    ,dateadd(day,-day(loaddate)+1,loaddate) BOM
    ,dateadd(month,12,BOM) Month12
    ,cast(dateadd(day,-day(ContMonth)+1,ContMonth) as date) TradeMonth
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where loaddate > cast('"""+lastDate+"""' as date)
    and division = 'domestic grain'
    and subdivision not in ('ag liquidity management', 'macro strategy', 'misc other', 'grain other', 'nag strategic hedge', 'grain exchange hedge', 'administration', 'dynamic hedging', 'pool program', 'strategic risk management', 'fuel hedge', 'dynamic hedge')
    and PositionType <> 'NPE - Contracts'
    )pos
    )aggpos
    where dayofweek(loaddate) not in (1,7)
    and loaddate not in ('2018-04-12', '2017-08-16', '2021-04-05')
    and poscommodity not in ('Currencies', 'Freight', '')
    and positionsubloc not in ('gives', 'takes', 'theoretical futures')
    and positionsubloc not like ('%foreign exchange%')
    and location not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, location, BeyondTwelveMonths, subdivision, trader, source
    
    order by loaddate, poscommodity
    """)
    tenor = tenor.append(tenor1)
        
 # tenor2 is for the specified subdivisions: 'Oilseed Products', 'Corn Products', 'Animal Products', 'Wheat Products', 'Mexico Ingredients', 'California Co-Products'
    tenor2 = sqlToDataFrame('SYBIQProd',"""
    select loaddate, poscommodity
    , case when source in ('DFI', 'AGR', 'TWS') and trader not in ('') then left(trader, 10) else location end trader
    , sum(abs(DeltaST))absST, BeyondTwelveMonths, subdivision
    from
    (
    Select loaddate, subdivision, poscommodity, DeltaST, DeltaPUOM, source, location
    , PositionSubLoc, PricePrefUOM, PriceUoMCode, PorS
    , MktPricePUOM, MktRegion, TradeDate, trader, ContMonth
    ,contractnumber, CustomerTypeDescription, Broker
    ,Month12, TradeMonth
    ,case when TradeMonth>Month12 then 1 else 0 end BeyondTwelveMonths
    from
    (
    
    SELECT loaddate, s.subdivisiondescription subdivision, d.divisiondescription division, positioncommoditygroupdescription poscommodity, DeltaQuantityShortTons DeltaST
    , DeltaquantityPreferredunitofmeasure DeltaPUOM, source, L.locationdescription location
    , positionsublocation PositionSubLoc, pricepreferredunitofmeasure PricePrefUOM, priceunitofmeasurecode PriceUoMCode, PurchaseorSale PorS
    , marketpricepreferredunitofmeasure MktPricePUOM, marketregiondescription MktRegion, TradeDate, traderdescription trader, contractmonth ContMonth
    ,contractnumber, CustomerTypeDescription, brokerdescription Broker, PositionType
    ,dateadd(day,-day(loaddate)+1,loaddate) BOM
    ,dateadd(month,12,BOM) Month12
    ,cast(dateadd(day,-day(ContMonth)+1,ContMonth) as date) TradeMonth
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where loaddate > cast('"""+lastDate+"""' as date)
    and subdivision in('Oilseed Products', 'Corn Products', 'Animal Products', 'Wheat Products', 'Mexico Ingredients', 'California Co-Products')
    and PositionType <> 'NPE - Contracts'
    )pos
    )aggpos
    where dayofweek(loaddate) not in (1,7)
    and poscommodity not in ('Currencies', 'Freight', '')
    and positionsubloc not in ('gives', 'takes', 'theoretical futures')
    and positionsubloc not like ('%foreign exchange%')
    and trader not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, trader, BeyondTwelveMonths, subdivision, source, location
    
    order by loaddate, poscommodity
    """)
    tenor = tenor.append(tenor2)
    
# tenor3 is for the subdivisions Italy, Spain, and Black Sea Trading    
    tenor3 = sqlToDataFrame('SYBIQProd',"""
    select loaddate, poscommodity
    --, location trader
    , case when source in ('DFI', 'AGR', 'TWS') and trader not in ('') then left(trader, 10) else location end trader
    , sum(abs(DeltaST))absST, BeyondTwelveMonths, subdivision
    from
    (
    Select loaddate, subdivision, poscommodity, DeltaST, DeltaPUOM, source, location
    , PositionSubLoc, PricePrefUOM, PriceUoMCode, PorS
    , MktPricePUOM, MktRegion, TradeDate, trader, ContMonth
    ,contractnumber, CustomerTypeDescription, Broker
    ,Month12, TradeMonth
    ,case when TradeMonth>Month12 then 1 else 0 end BeyondTwelveMonths
    from
    (
    
    SELECT loaddate, s.subdivisiondescription subdivision, d.divisiondescription division, positioncommoditygroupdescription poscommodity, DeltaQuantityShortTons DeltaST
    , DeltaquantityPreferredunitofmeasure DeltaPUOM, source, L.locationdescription location
    , positionsublocation PositionSubLoc, pricepreferredunitofmeasure PricePrefUOM, priceunitofmeasurecode PriceUoMCode, PurchaseorSale PorS
    , marketpricepreferredunitofmeasure MktPricePUOM, marketregiondescription MktRegion, TradeDate, traderdescription trader, contractmonth ContMonth
    ,contractnumber, CustomerTypeDescription, brokerdescription Broker
    ,dateadd(day,-day(loaddate)+1,loaddate) BOM
    ,dateadd(month,12,BOM) Month12
    ,cast(dateadd(day,-day(ContMonth)+1,ContMonth) as date) TradeMonth
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where loaddate > cast('"""+lastDate+"""' as date)
    and subdivision in('italy', 'spain', 'black sea trading')
    )pos
    )aggpos
    where dayofweek(loaddate) not in (1,7)
    and poscommodity not in ('Currencies', 'Freight', '')
    and positionsubloc not in ('gives', 'takes', 'theoretical futures')
    and positionsubloc not like ('%foreign exchange%')
    and location not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, location, BeyondTwelveMonths, subdivision, source, trader
    
    order by loaddate, poscommodity
    """)
    tenor = tenor.append(tenor3)
 
# brazil tenor is for the subdivision Brazil grain   
    braziltenor = sqlToDataFrame('SYBIQProd',"""
    select loaddate, poscommodity
    , case when source in ('DFI', 'AGR', 'TWS') and trader not in ('') then left(trader, 10) else location end trader
    --, location trader
    , sum(abs(DeltaST))absST, BeyondTwelveMonths, subdivision
    from
    (
    Select loaddate, subdivision, poscommodity, DeltaST, DeltaPUOM, source, location
    , PositionSubLoc, PricePrefUOM, PriceUoMCode, PorS
    , MktPricePUOM, MktRegion, TradeDate, trader, ContMonth
    ,contractnumber, CustomerTypeDescription, Broker
    ,Month12, TradeMonth
    ,case when TradeMonth>Month12 then 1 else 0 end BeyondTwelveMonths
    from
    (
    
    SELECT loaddate, s.subdivisiondescription subdivision, d.divisiondescription division, positioncommoditygroupdescription poscommodity, DeltaQuantityShortTons DeltaST
    , DeltaquantityPreferredunitofmeasure DeltaPUOM, source, L.locationdescription location
    , positionsublocation PositionSubLoc, pricepreferredunitofmeasure PricePrefUOM, priceunitofmeasurecode PriceUoMCode, PurchaseorSale PorS
    , marketpricepreferredunitofmeasure MktPricePUOM, marketregiondescription MktRegion, TradeDate, traderdescription trader, contractmonth ContMonth
    ,contractnumber, CustomerTypeDescription, brokerdescription Broker
    ,dateadd(day,-day(loaddate)+1,loaddate) BOM
    ,dateadd(month,12,BOM) Month12
    ,cast(dateadd(day,-day(ContMonth)+1,ContMonth) as date) TradeMonth
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where loaddate > cast('"""+lastDate+"""' as date)
    and subdivision in('brazil grain')
    )pos
    )aggpos
    where dayofweek(loaddate) not in (1,7)
    and poscommodity not in ('Currencies', 'Freight', '')
    and positionsubloc not in ('gives', 'takes', 'theoretical futures')
    and positionsubloc not like ('%foreign exchange%')
    and trader not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, location, BeyondTwelveMonths, subdivision, source, trader
    
    order by loaddate, poscommodity
    """)
    tenor = tenor.append(braziltenor)
    tenor.columns = tenor.columns.str.lower()
    tenor = tenor.rename(columns = {'absst':'absST', 'beyondtwelvemonths':'BeyondTwelveMonths'})
    
    tenor = pd.DataFrame(tenor.groupby(['loaddate', 'poscommodity', 'trader', 'subdivision'])['absST'].sum())
    tenor = tenor.reset_index()
    
    tenor2 = tenor.loc[tenor['BeyondTwelveMonths']==1]
    tenor2 = tenor2.rename(columns = {'absST':'outsideVal'})
    
    alltenor = tenor.merge(tenor2, how = 'left', left_on = ['loaddate', 'poscommodity', 'trader', 'subdivision'], right_on = ['loaddate', 'poscommodity', 'trader', 'subdivision'])
    alltenor['BeyondTwelveMonths'] = alltenor['BeyondTwelveMonths'].fillna(0)
    alltenor['absST'] = alltenor['absST'].fillna(0)    
    alltenor['outsideVal'] = alltenor['outsideVal'].fillna(0)

    alltenor_calc = alltenor.loc[alltenor['outsideVal']>0]
    alltenor_calc['calc'] = alltenor_calc['outsideVal'] / alltenor['absST']
    alltenor_calc.loc[alltenor_calc['calc']>.2, 'breaks'] = 1
    alltenor_calc['breaks'] = alltenor_calc['breaks'].fillna(0)

    alltenor2 = alltenor.merge(alltenor_calc, how = 'left', left_on = ['loaddate', 'poscommodity', 'trader', 'subdivision', 'absST', 'outsideVal', 'BeyondTwelveMonths'], right_on =  ['loaddate', 'poscommodity', 'trader', 'subdivision', 'absST', 'outsideVal', 'BeyondTwelveMonths'])
    alltenor2['calc'] = alltenor2['calc'].fillna(0)
    alltenor2['breaks'] = alltenor2['breaks'].fillna(0)
    alltenor2['test'] = 'tenor breaks'
    alltenor2 = alltenor2.rename(columns = {'calc':'dailyValue'})
    alltenor2 = alltenor2[['loaddate', 'subdivision', 'trader', 'poscommodity', 'dailyValue', 'breaks', 'test']]
    
    alltenor2.info()
    alltenor2['breaks'] = alltenor2['breaks'].astype(int)
    alltenor3 = pd.DataFrame(alltenor2.groupby(['loaddate', 'subdivision', 'trader', 'poscommodity', 'breaks', 'test'])['dailyValue'].sum())
    alltenor3 = alltenor3.reset_index()
    
    alltrades = pd.DataFrame()
    alltrades = alltrades.append(alltenor3)
    alltrades = alltrades[['loaddate', 'subdivision', 'trader', 'poscommodity', 'dailyValue', 'breaks', 'test']]
    
    
    query = sqlToDataFrame('CentralHedgePRD',"""
    select pos.TradeDate, Pos.Account, Sub.SubDivisionDescription, Loc.LocationDescription, commcode.commgrp Commodity,  pos.DayTradeQty, pos.daytradePnL
    from
    (
    select * --distinct commodity
    from
    (
    select pos.*, mktcode.daytradefilter
    from
    (
      select *, case when purchaseQty>daytradeqty then ((daytradeqty/purchaseqty)*PurchasePnL)+SalePnL else ((daytradeqty/saleqty)*SalePnL)+PurchasePnL end daytradePnL 
    from
    (
    select purchase.*, sale.SaleQty, sale.SalePnL, case when purchase.purchaseqty<sale.saleqty then purchase.purchaseqty else sale.saleqty end DayTradeQty
    from
    (
    select TradeDate, account,subdivision,location, Commodity, source, PositionMonth,fintype, buysellcode PorS,AccountName, StrikePrice, sum(GrossQty)PurchaseQty, sum(MTM)Purchasepnl
    from [CentralHedge].[dbo].[FCMFees]
    where buysellcode='B'
    and account not in ('CTG-CHDGE','FLR-CHDGE','GRN-53137','GRN-53138')
    and account not like '056%'
    and subdivision not like 'Central Hedge%'
    and BrokerNum <>'EFP'
    and CardNumber <> 'CHD'
    and loaddate > cast('"""+lastDate+"""' as date)
    and commentcode not in ('K','I',' I')
    --and accountname<>'Fuel Hedge'
    and loaddate=tradedate
    and source <> 'STX'
    and grossqty<>0
    group by TradeDate, account,subdivision,location, Commodity, source, PositionMonth,fintype, buysellcode,AccountName,StrikePrice
    )purchase
    
    left join
    (select--*
    TradeDate, account,subdivision,location, Commodity, source, PositionMonth, fintype, buysellcode PorS,AccountName,StrikePrice, sum(GrossQty)SaleQty, sum(MTM)Salepnl 
    from [CentralHedge].[dbo].[FCMFees]
    where buysellcode='S'
    and account not in ('CTG-CHDGE','FLR-CHDGE','GRN-53137','GRN-53138')
    --and mktcode in (select * from #mktcode)
    and account not like '056%'
    and BrokerNum <>'EFP'
    and CardNumber <> 'CHD'
    and subdivision not like 'Central Hedge%'
    and loaddate > cast('"""+lastDate+"""' as date)
    and commentcode not in ('K','I',' I')
   -- and accountname<>'Fuel Hedge'
    and loaddate=tradedate
    and source <> 'STX'
    and grossqty<>0
    group by TradeDate, account,subdivision,location, Commodity, source, PositionMonth,fintype, buysellcode,AccountName,StrikePrice
    )sale
    on purchase.Tradedate=sale.tradedate and purchase.commodity=sale.commodity and purchase.account=sale.account and purchase.PositionMonth=sale.PositionMonth and purchase.fintype = sale.Fintype and purchase.StrikePrice=sale.strikeprice
    )PorS
    --where SaleQty is not null
    
    
    union all 
    
    select TradeDate, Account, null subdivision, null location,  commodity, source, null positionmonth, null fintype, null PorS, AccountName,null StrikePrice, 0 PurchaseQty, 0 PurchasePnL
    ,0 SaleQty, 0 SalePnL, 0 daytradeqty, 0 daytradepnl from
    (
    select distinct  account,  accountname, commodity, source
    from [CentralHedge].[dbo].[FCMFees]
    where account not in ('CTG-CHDGE','FLR-CHDGE','GRN-53137','GRN-53138')
    and account not like '056%'
    and subdivision not like 'Central Hedge%'
    and BrokerNum <>'EFP'
    and CardNumber <> 'CHD'
    and loaddate > cast('"""+lastDate+"""' as date)
    and loaddate=tradedate
    and source <> 'STX'
    and accountname not in ('CSO - Carry Structur','HERMOSILLO SBM SG','GAVILON GLOBAL AG H')
    and grossqty<>0
    )acct
    cross join
    (select tradedate
    from [CentralHedge].[dbo].[FCMFees]
     
    where loaddate > cast('"""+lastDate+"""' as date)
    and tradedate>= loaddate
    group by tradedate
    )dt
    )pos
    
    left join
    (
    select account mktfilter, sum(daytradeqty) daytradefilter
    from
    (
    select *, case when purchaseQty>daytradeqty then ((daytradeqty/purchaseqty)*PurchasePnL)+SalePnL else ((daytradeqty/saleqty)*SalePnL)+PurchasePnL end daytradePnL 
    from
    (
    select purchase.*, sale.SaleQty, sale.SalePnL, case when purchase.purchaseqty<sale.saleqty then purchase.purchaseqty else sale.saleqty end DayTradeQty
    from
    (
    select TradeDate, account,subdivision,location, Commodity, source, PositionMonth,fintype, buysellcode PorS,AccountName, StrikePrice, sum(GrossQty)PurchaseQty, sum(MTM)Purchasepnl
    from [CentralHedge].[dbo].[FCMFees] 
    where buysellcode='B'
    and loaddate > cast('"""+lastDate+"""' as date)
    and account not in ('CTG-CHDGE','FLR-CHDGE','GRN-53137','GRN-53138')
    and account not like '056%'
    and BrokerNum <>'EFP'
    and CardNumber <> 'CHD'
    and subdivision not like 'Central Hedge%'
    and loaddate=tradedate
    and source <> 'STX'
    and grossqty<>0
    and commentcode not in ('K','I',' I')
    group by TradeDate, account,subdivision,location, Commodity, source, PositionMonth,fintype, buysellcode,AccountName,StrikePrice
    )purchase
    
    left join
    (select-- *
    TradeDate, account,subdivision,location, Commodity, source, PositionMonth,fintype, buysellcode PorS,AccountName,StrikePrice, sum(GrossQty)SaleQty, sum(MTM)Salepnl
    from [CentralHedge].[dbo].[FCMFees]
    where buysellcode='S'
    and loaddate > cast('"""+lastDate+"""' as date)
    and account not in ('CTG-CHDGE','FLR-CHDGE','GRN-53137','GRN-53138')
    and account not like '056%'
    and BrokerNum <>'EFP'
    and CardNumber <> 'CHD'
    and subdivision not like 'Central Hedge%'
    and commentcode not in ('K','I',' I')
    and loaddate=tradedate
    and source <> 'STX'
    and grossqty<>0
    group by TradeDate, account,subdivision,location, Commodity, source, PositionMonth,fintype, buysellcode,AccountName,StrikePrice
    )sale
    on purchase.Tradedate=sale.tradedate and purchase.commodity=sale.commodity and purchase.account=sale.account and purchase.PositionMonth=sale.PositionMonth and purchase.fintype = sale.Fintype and purchase.StrikePrice=sale.strikeprice
    )PorS
    )acct
    group by account
    )mktcode
    on mktfilter=account
    where mktfilter is not null
    and accountname not in ('CSO - Carry Structur','HERMOSILLO SBM SG','GAVILON GLOBAL AG H')
    and tradedate > '2015-01-01'
    )pos
    )pos
    left join
    (
    select distinct commoditydescription, commoditygroupdescription commgrp
    from
    (
    SELECT  --[Id]
         -- ,[CommodityCode]
          [CommodityDescription]
          --,[UnitOfMeasureId]
          ,[CommoditySubGroupId]
       
      FROM [TEPSQL040].[MasterData].[dbo].[Commodity]
      where statusid='1' --commoditycode='CYELLOW'
      )com
      left join
      (SELECT  [Id]
         
          ,[CommodityGroupId]
         
      FROM [TEPSQL040].[MasterData].[dbo].[CommoditySubGroup]
      where statusid='1'
      )comsub
      on com.CommoditySubGroupId=comsub.id
      left join
      (SELECT  [Id]
          ,[CommodityGroupDescription]      
      FROM [TEPSQL040].[MasterData].[dbo].[CommodityGroup]
      where statusid='1'
      )comgrp
      on comsub.CommodityGroupId=comgrp.id
       where (case when commoditydescription='Corn' and commoditygroupdescription='Corn Products' then 0 else 1 end)=1
      )commcode
      
      on commcode.commoditydescription = pos.commodity
     
     inner join
     TEPSQL040.MasterData.dbo.ReportStructureMap RSM
                    ON account = RSM.SourceReportCode
    				and case when source='FMT' then '3' when source='WFC' then '67' else '0' end = RSM.SourceID
                       AND RSM.SourceId in ('67','3')
                INNER JOIN TEPSQL040.MasterData.dbo.Locations Loc
                    ON RSM.LocationId = Loc.Id
                INNER JOIN TEPSQL040.MasterData.dbo.SubDivision Sub
                    ON Loc.SubDivisionId = Sub.Id
    
    where locationDescription not like '%GPS%'
         """)

    daytrades = query.copy()    
    daytrades = daytrades.loc[(daytrades['SubDivisionDescription'] != 'Test') | (daytrades['SubDivisionDescription'] != 'Pool Program') | (daytrades['SubDivisionDescription'] != 'Grain Exchange Hedge') | (daytrades['SubDivisionDescription'] != 'Brazil Farm Program') | (daytrades['SubDivisionDescription'] != 'AgYield')]
    
    daytrades['DayTradeQty'] = daytrades['DayTradeQty'].fillna(0)    
    daytrades['daytradePnL'] = daytrades['daytradePnL'].fillna(0)    
    daytrades = daytrades.loc[daytrades['Commodity'] != 'Currencies']
    daytrades = daytrades.rename(columns = {'TradeDate':'loaddate', 'SubDivisionDescription':'subdivision', 'Commodity':'poscommodity', 'LocationDescription':'trader', 'DayTradeQty':'dailyValue'})

    daytrades = daytrades.groupby(['loaddate', 'subdivision', 'poscommodity','trader']).sum()
    daytrades = daytrades.reset_index()

    daytrades['breaks'] = daytrades.apply(lambda x: 1 if x['dailyValue'] > 100 else 0, axis = 1)

    daytrades['test'] = 'day trade breaks'
    daytrades = daytrades[['loaddate', 'subdivision', 'trader', 'poscommodity', 'dailyValue', 'breaks', 'test']]
    
    alltrades = alltrades.append(daytrades)
    
    PorS = pd.DataFrame()
    
    # 4/11 added case statement to include NA because solely purchase and sales resulted in missing dates because inventory is classified as NA
    trades = sqlToDataFrame('SYBIQProd',"""
    select loaddate, positioncommoditygroupdescription poscommodity
    , case when source in ('DFI', 'AGR', 'TWS') and traderdescription not in ('') then left(traderdescription,10) else l.locationdescription end trader
    , sum(case when PurchaseorSale = 'NA' then DeltaQuantityShortTons else abs(DeltaQuantityShortTons) end) absST
    , PurchaseorSale PorS, s.subdivisiondescription subdivision
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where contractnumber not in ('''20-C46551')
    and subdivision in('Oilseed Products', 'Corn Products', 'Animal Products', 'Wheat Products', 'Mexico Ingredients', 'California Co-Products')
    and positionsublocation not in ('Gives','Takes','Theoretical Future','Invoice','AR','Loan','Cash','Foreign Exchange')
    and loaddate > cast('"""+startDate+"""' as date)
    and dayofweek(loaddate) not in (1,7)
    and loaddate not in ('2018-04-12', '2017-08-16', '2021-04-05')
    and poscommodity not in ('Currencies', 'Freight', '')
    --and PorS not in ('NA')
    and not (contractnumber = '''' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''B010109' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''10-P46466' and loaddate = '2017-09-27' and PorS = 'Purchase' and poscommodity = 'distillers dried grains' and trader like '%matan nadle%')
    and not (contractnumber = '''B010197' and loaddate = '2016-08-05' and PorS = 'Purchase' and poscommodity = 'corn' and L.locationdescription= 'burley, id')
    and not (loaddate = '2017-07-03' and PorS = 'Purchase' and poscommodity = 'Feedgrains' and trader = 'Spain')
    and not (loaddate = '2019-10-21' and PorS = 'Sale' and poscommodity = 'feedgrains' and trader = 'italy')
    and trader not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, trader, PorS, subdivision
    order by loaddate
    """)
    
    domestic = sqlToDataFrame('SYBIQProd',"""
    select loaddate, positioncommoditygroupdescription poscommodity
    , case when source in ('DFI', 'AGR', 'TWS') and traderdescription not in ('') then left(traderdescription,10) else l.locationdescription end trader
    --, l.locationdescription trader
    , sum(case when PurchaseorSale = 'NA' then DeltaQuantityShortTons else abs(DeltaQuantityShortTons) end) absST
    , PurchaseorSale PorS, s.subdivisiondescription subdivision
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where contractnumber not in ('''20-C46551')
    and d.divisiondescription = 'domestic grain'
    and subdivision not in ('ag liquidity management', 'macro strategy', 'misc other', 'grain other', 'nag strategic hedge', 'grain exchange hedge', 'administration', 'dynamic hedging', 'pool program', 'strategic risk management', 'fuel hedge', 'dynamic hedge')
    and positionsublocation not in ('Gives','Takes','Theoretical Future','Invoice','AR','Loan','Cash','Foreign Exchange')
    and loaddate > cast('"""+startDate+"""' as date)
    and dayofweek(loaddate) not in (1,7)
    and poscommodity not in ('Currencies', 'Freight', '')
    --and PorS not in ('NA')
    and not (contractnumber = '''' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''B010109' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''10-P46466' and loaddate = '2017-09-27' and PorS = 'Purchase' and poscommodity = 'distillers dried grains' and trader like '%matan nadle%')
    and not (contractnumber = '''B010197' and loaddate = '2016-08-05' and PorS = 'Purchase' and poscommodity = 'corn' and L.locationdescription= 'burley, id')
    and not (loaddate = '2017-07-03' and PorS = 'Purchase' and poscommodity = 'Feedgrains' and trader = 'Spain')
    and not (loaddate = '2019-10-21' and PorS = 'Sale' and poscommodity = 'feedgrains' and trader = 'italy')
    and trader not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, trader, PorS, subdivision
    order by loaddate
    """)
    
    italyspain = sqlToDataFrame('SYBIQProd', """
    select loaddate, positioncommoditygroupdescription poscommodity
    , case when source in ('DFI', 'AGR', 'TWS') and traderdescription not in ('') then left(traderdescription,10) else l.locationdescription end trader
    --, l.locationdescription trader
    , sum(case when PurchaseorSale = 'NA' then DeltaQuantityShortTons else abs(DeltaQuantityShortTons) end) absST
    , PurchaseorSale PorS, s.subdivisiondescription subdivision
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where contractnumber not in ('''20-C46551')
    and subdivision in ('italy', 'spain', 'black sea trading')
    and positionsublocation not in ('Gives','Takes','Theoretical Future','Invoice','AR','Loan','Cash','Foreign Exchange')
    and loaddate > cast('"""+startDate+"""' as date)
    and dayofweek(loaddate) not in (1,7)
    and poscommodity not in ('Currencies', 'Freight', '')
    --and PorS not in ('NA')
    and not (contractnumber = '''' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''B010109' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''10-P46466' and loaddate = '2017-09-27' and PorS = 'Purchase' and poscommodity = 'distillers dried grains' and trader like '%matan nadle%')
    and not (contractnumber = '''B010197' and loaddate = '2016-08-05' and PorS = 'Purchase' and poscommodity = 'corn' and L.locationdescription= 'burley, id')
    and not (loaddate = '2017-07-03' and PorS = 'Purchase' and poscommodity = 'Feedgrains' and trader = 'Spain')
    and not (loaddate = '2019-10-21' and PorS = 'Sale' and poscommodity = 'feedgrains' and trader = 'italy')
    and trader not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, trader, PorS, subdivision
    order by loaddate
    """)
    
    brazilgrain = sqlToDataFrame('SYBIQProd', """
    select loaddate, positioncommoditygroupdescription poscommodity
    , case when source in ('DFI', 'AGR', 'TWS') and traderdescription not in ('') then left(traderdescription,10) else l.locationdescription end trader
    --, l.locationdescription trader
    , sum(case when PurchaseorSale = 'NA' then DeltaQuantityShortTons else abs(DeltaQuantityShortTons) end) absST
    , PurchaseorSale PorS, s.subdivisiondescription subdivision
    from position.allpositionsfinal v
    
    LEFT JOIN masterdata.locations l on l.LocationCode = v.LocationCode
    LEFT JOIN masterdata.subdivision s on s.subdivisioncode = l.SubDivisionCode
    LEFT JOIN masterdata.division d on d.divisioncode = s.DivisionCode
    
    right join position.gavilonworkdays gw on v.loaddate = gw.workdays
    
    where contractnumber not in ('''20-C46551')
    and subdivision in ('brazil grain')
    and positionsublocation not in ('Gives','Takes','Theoretical Future','Invoice','AR','Loan','Cash','Foreign Exchange')
    and loaddate > cast('"""+startDate+"""' as date)
    and dayofweek(loaddate) not in (1,7)
    and poscommodity not in ('Currencies', 'Freight', '')
    --and PorS not in ('NA')
    and not (contractnumber = '''' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''B010109' and loaddate = '2018-09-26' and poscommodity = 'corn' and L.locationdescription = 'dimmitt, tx')
    and not (contractnumber = '''10-P46466' and loaddate = '2017-09-27' and PorS = 'Purchase' and poscommodity = 'distillers dried grains' and trader like '%matan nadle%')
    and not (contractnumber = '''B010197' and loaddate = '2016-08-05' and PorS = 'Purchase' and poscommodity = 'corn' and L.locationdescription= 'burley, id')
    and not (loaddate = '2017-07-03' and PorS = 'Purchase' and poscommodity = 'Feedgrains' and trader = 'Spain')
    and not (loaddate = '2019-10-21' and PorS = 'Sale' and poscommodity = 'feedgrains' and trader = 'italy')
    and trader not in ('Hedge Desk (Cent.)', 'GPS - Accumulators', 'GPS - Avg Price', 'GPS - Bonus Target', 'GPS - Customer Options', 'GPS - Min Avg Price', 'GPS - Producers Choice', 'GPS - Smart Avg')
    group by loaddate, poscommodity, trader, PorS, subdivision
    order by loaddate
    """)
    
    PorS = PorS.append([italyspain, domestic, trades, brazilgrain])
    PorS.columns = PorS.columns.str.lower()
    PorS = PorS.rename(columns = {'absst':'absST', 'pors':'PorS'})
    
    ps = PorS.pivot_table(index=['loaddate','poscommodity','trader', 'subdivision'],columns='PorS',values='absST')
    ps = ps.reset_index()
    ps = ps.fillna(0.0)
    ps['PnS'] = ps['Purchase'] + ps['Sale'] + ps['NA']
    ps = ps[['loaddate', 'poscommodity', 'trader', 'subdivision', 'PnS']]

    ps['dailyValue'] = round(ps['PnS'],5)
#    ps['75day Moving Avg'] = round(ps.groupby(['poscommodity','trader'])['dailyValue'].transform(lambda x: x.rolling(window =75).mean()),5)
#    ps['75day st dev'] = round(ps.groupby(['poscommodity','trader'])['dailyValue'].transform(lambda x: x.rolling(window =75).std()),5)
#    ps['75day upper bound'] = round(ps['75day Moving Avg'] + (ps['75day st dev']*2) ,5)
#    ps['breaks'] = ps.apply(lambda x: 1 if x['dailyValue'] > x['75day upper bound'] else 0, axis=1)
#    ps['test'] = 'PnS breaks'
#    ps = ps[ps['75day Moving Avg'].notna()]
#    ps= ps[['loaddate', 'subdivision', 'trader', 'poscommodity', 'dailyValue', 'breaks', 'test']]
    
#   changed to this code 4/11 from 75 day moving avg/std -- daily value breaking upper bound. this tracks the new highs better  
    ps['maxValue'] = ps.groupby(['poscommodity', 'trader'])['dailyValue'].transform(lambda x: x.rolling(window = 252).max())
    ps['newmax'] = ps.apply(lambda x: 1 if x['dailyValue'] >= x['maxValue'] else 0, axis=1)
    ps['prevDailyVal'] = ps.groupby(['poscommodity', 'trader'])['dailyValue'].shift(1)
    ps.loc[ps['maxValue'] == ps['prevDailyVal'], 'newmax'] = 0
    ps.loc[ps['maxValue']== 0, 'newmax'] = 0
    ps['test'] = 'PnS breaks'
    ps['count avgs breakage'] = ps.groupby(['poscommodity','trader'])['newmax'].cumsum()
    #pRatio = pRatio.dropna(axis=0)
    ps = ps.rename(columns = {'newmax':'breaks'})
    ps = ps[['loaddate', 'subdivision', 'trader', 'poscommodity', 'dailyValue', 'breaks', 'test']]
    alltrades = alltrades.append(ps)

    alltrades['loaddate'] = pd.to_datetime(alltrades['loaddate']).dt.date
    alltrades = alltrades.loc[alltrades['loaddate'] > startDate2]
    
    alltrades.info()
        
    dataFrameToSQL('RiskPOC', alltrades, 'newcombinedtrades2', "//tedfil01/DataDropDEV/PythonPOC/Upload_CSVs/newcombinedtrades2.csv", True)
    
    subProcessName='Job Complete'
        
    processLog(processName, subProcessName,None)

except Exception as e:

    print (' data parsing failed!')

    processLog(processName, subProcessName, errorLog = str(e))
 
#datainsert = pd.DataFrame()
#datainsert = datainsert.append(alltenor2)
#datainsert = datainsert.loc[datainsert['loaddate'] <= datemax]
#datainsert = datainsert.loc[datainsert['loaddate'] >= datemin]
#
#alltenor2 = alltenor2.loc[alltenor2['loaddate'] <= datemax]
#alltenor2= alltenor2.loc[alltenor2['loaddate'] >= datemin]
#ps2 = ps2.loc[ps2['loaddate'] <= datemax]
#ps2 = ps2.loc[ps2['loaddate'] >= datemin]
#
#dataFrameToSQL('RiskPOC', ps2, 'newcombinedtrades', "//tedfil01/DataDropDEV/PythonPOC/Upload_CSVs/newcombinedtrades.csv", True)

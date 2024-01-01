#==============================================================

from pandas import DataFrame
from pandas import read_csv

#==============================================================

PATH_ROOT = "tutorial_dags/sehat_data/"
PATH_PRODUCT = PATH_ROOT+"/sehat_product.csv"
PATH_PURCHASE_AUGUST = PATH_ROOT+"/sehat_purchase_august.csv"
PATH_SALES_AUGUST = PATH_ROOT+"/sehat_sales_august.csv"
PATH_BI_AUGUST = PATH_ROOT+"/sehat_BI_august.csv"
PATH_COGAS_AUGUST = PATH_ROOT+"statement/sehat_cogas_august.csv"
PATH_COGS_AUGUST = PATH_ROOT+"statement/sehat_cogs_august.csv"
PATH_STATEMENT_AUGUST = PATH_ROOT+"statement/sehat_statement_august.csv"
PATH_BI_SEPTEMBER = PATH_ROOT+"statement/sehat_BI_september.csv"

#==============================================================

# Task yang harus dijalanken ===

def start():
    print("Starting...")   
    
    df_statement_august = DataFrame({
        'ACCOUNT':['NET_SALES','COGS','GROSS_PROFIT','COGAS','ENDING_INVENTORY'],
        'VALUE':0
    })
    
    df_statement_august.to_csv(path_or_buf=PATH_STATEMENT_AUGUST, sep=',', index=False)
    
def countCOGAS_BI():
    df_product = read_csv(filepath_or_buffer=PATH_PRODUCT, sep=",")
    df_BI_august = read_csv(filepath_or_buffer=PATH_BI_AUGUST, sep=",")
        
    # Data Proccessing        
    df_cogas_august = DataFrame({
        'product_id' : df_product['product_id'],
        'product_cost_unit' : df_product['product_cost'],
        'product_quantity' : df_BI_august['product_quantity']
    })         
    
    df_cogas_august.index = df_cogas_august['product_id']
    
    df_cogas_august['product_cost_total'] = df_cogas_august['product_cost_unit'] * df_cogas_august['product_quantity']
    
    # print(df_cogas_august)
    
    # Data to CSV
    df_cogas_august.to_csv(path_or_buf=PATH_COGAS_AUGUST, sep=",", index=False)

def countCOGAS_purchase():
    df_cogas_august = read_csv(filepath_or_buffer=PATH_COGAS_AUGUST,sep=",")
    df_purchase_august = read_csv(filepath_or_buffer=PATH_PURCHASE_AUGUST,sep=",")
    
    # Data Proccessing
    df_cogas_august.index = df_cogas_august['product_id']

    for product, quantity in zip(df_purchase_august['product_id'], df_purchase_august['purchase_quantity']):
        df_cogas_august.loc[product,'product_quantity'] += quantity
    
    df_cogas_august['product_cost_total'] = df_cogas_august['product_cost_unit'] * df_cogas_august['product_quantity']
            
    # print(df_cogas_august)
    
    # Data to CSV
    df_cogas_august.to_csv(path_or_buf=PATH_COGAS_AUGUST, sep=",", index=False)
    
    # Create COGAS Statement
    df_statement_august = read_csv(filepath_or_buffer=PATH_STATEMENT_AUGUST, sep=',')
    df_statement_august.index = df_statement_august['ACCOUNT']
    df_statement_august.loc['COGAS','VALUE'] = df_cogas_august['product_cost_total'].sum()
    df_statement_august.to_csv(path_or_buf=PATH_STATEMENT_AUGUST, index=False, sep=',')
    
def countCOGS():
    df_sales_august = read_csv(filepath_or_buffer=PATH_SALES_AUGUST,sep=",")
    df_product = read_csv(filepath_or_buffer=PATH_PRODUCT,sep=",")
    
    # Data Proccessing
    df_cogs_august = DataFrame({
        'product_id' : df_product['product_id'],
        'product_cost' : df_product['product_cost'],
        'sales_quantity' : 0
    })
    
    df_cogs_august.index = df_cogs_august['product_id']
    
    for product, quantity in zip(df_sales_august['product_id'], df_sales_august['sales_quantity']):
        df_cogs_august.loc[product, 'sales_quantity'] += quantity
    
    df_cogs_august['sales_cost_total'] = df_cogs_august['sales_quantity'] * df_cogs_august['product_cost']
    
    # print(df_cogs_august)
    
    # Data to CSV
    df_cogs_august.to_csv(path_or_buf=PATH_COGS_AUGUST, sep=",", index=False)
    
    # Create COGS Statement
    df_statement_august = read_csv(filepath_or_buffer=PATH_STATEMENT_AUGUST, sep=',')
    df_statement_august.index = df_statement_august['ACCOUNT']
    df_statement_august.loc['COGS','VALUE'] = df_cogs_august['sales_cost_total'].sum()
    df_statement_august.to_csv(path_or_buf=PATH_STATEMENT_AUGUST, index=False, sep=',')
    
def countNS():
    df_product = read_csv(filepath_or_buffer=PATH_PRODUCT, sep=',')
    df_sales_august = read_csv(filepath_or_buffer=PATH_SALES_AUGUST, sep=',')
    
    # Proses Data
    df_netsales_august = DataFrame({
        'product_id' : df_product['product_id'],
        'product_price' : df_product['product_price'],
        'sales_quantity' : 0,
        'sales_price_total' : 0
    })
    
    df_netsales_august.index = df_netsales_august['product_id']
    
    for product, quantity in zip(df_sales_august['product_id'], df_sales_august['sales_quantity']):
        df_netsales_august.loc[product, 'sales_quantity'] += quantity
    
    df_netsales_august['sales_price_total'] = df_netsales_august['sales_quantity'] * df_netsales_august['product_price']
    
    # print(df_netsales_august)
    
    # Data to CSV
    df_netsales_august.to_csv(path_or_buf=PATH_COGS_AUGUST, sep=",", index=False)
    
    # Create Net Sales Statement
    df_statement_august = read_csv(filepath_or_buffer=PATH_STATEMENT_AUGUST, sep=',')
    df_statement_august.index = df_statement_august['ACCOUNT']
    df_statement_august.loc['NET_SALES','VALUE'] = df_netsales_august['sales_price_total'].sum()
    df_statement_august.to_csv(path_or_buf=PATH_STATEMENT_AUGUST, index=False, sep=',')
       
def countEI():
    # Create EI Statement
    df_statement_august = read_csv(filepath_or_buffer=PATH_STATEMENT_AUGUST, sep=',')
    df_statement_august.index = df_statement_august['ACCOUNT']
    df_statement_august.loc['ENDING_INVENTORY','VALUE'] = df_statement_august.loc['COGAS','VALUE'] - df_statement_august.loc['COGS','VALUE']
    df_statement_august.to_csv(path_or_buf=PATH_STATEMENT_AUGUST, index=False, sep=',')
    
    # Proccess BI for next month
    df_cogs_august = read_csv(filepath_or_buffer=PATH_COGS_AUGUST, sep=',')
    df_cogas_august = read_csv(filepath_or_buffer=PATH_COGAS_AUGUST, sep=',')
    
    df_BI_september = DataFrame({
        'product_id': df_cogs_august['product_id'],
        'product_quantity': df_cogas_august['product_quantity'] - df_cogs_august['sales_quantity']
    })
    
    df_BI_september.to_csv(path_or_buf=PATH_BI_SEPTEMBER, sep=',', index=False)
    
def countGP():
    # Create Gross Profit Statement
    df_statement_august = read_csv(filepath_or_buffer=PATH_STATEMENT_AUGUST, sep=',')
    df_statement_august.index = df_statement_august['ACCOUNT']
    df_statement_august.loc['GROSS_PROFIT','VALUE'] = df_statement_august.loc['NET_SALES','VALUE'] - df_statement_august.loc['COGS','VALUE']
    df_statement_august.to_csv(path_or_buf=PATH_STATEMENT_AUGUST, index=False, sep=',')
    
def finish():
    print("Finishing...")  

#==============================================================
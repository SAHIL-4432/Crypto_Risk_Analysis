from sqlalchemy import create_engine
from config import DB_CONFIG

def store_data(df, table_name):
    url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(url)
    df.to_sql(table_name, engine, if_exists='append', index=False)

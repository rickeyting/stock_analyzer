import pandas as pd
import psycopg2
from sqlalchemy import create_engine



def init_tables(engine):
    init_tables = {
            'table1': {'cols':['col1','col2'],
                     'primaries': ['col1']
                    },
            }
            
    for i in init_tables:
        table = init_tables[i]
        df = pd.DataFrame(columns = table['cols'])
        df.to_sql(table, engine, index=False)
        query = "ALTER TABLE {} ADD PRIMARY KEY (".format(i)
        for j in table['primaries']:
            query += j
            if table['primaries'].index(j) != len(table['primaries']) -1:
                query += ", "
        query += ");"
        engine.execute(query)
    
    return 'tables created'

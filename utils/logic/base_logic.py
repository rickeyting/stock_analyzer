import pandas as pd
import yaml
from tqdm import tqdm

from utils.logic.technical import technical_analysis
from utils.postgredb import db_get_stock_id
from utils.backtesting import trace_back


BASE_YAML = r'C:\Users\mick7\PycharmProjects\stock_analyzer\New folder\stock_analyzer\utils\logic\base.yaml'


def init_acc(base_yaml=BASE_YAML):
    stock_id_list = db_get_stock_id()
    with open(base_yaml, 'r', encoding='utf-8-sig') as file:
        technical_list = yaml.safe_load(file)
    for t in technical_list:
        for name in technical_list[t]:
            final_ans = []
            for i in tqdm(stock_id_list):
                df = technical_analysis(t, technical_list[t][name], i)
                df.loc[df.ans > 80, 'predicted'] = 'sell'
                df.loc[df.ans < 20, 'predicted'] = 'buy'
                df = df.dropna()
                df = df[['date', 'predicted']]
                result = trace_back(df, i)
                final_ans.append(result)
            final_ans = pd.DataFrame(final_ans, columns=['TP', 'FP', 'FN', 'TN', 'Pneutral', 'Nneutral'])
            final_ans = final_ans.sum()
            print(name, 'buy_acc:{}'.format(final_ans.TP/(final_ans.TP+final_ans.FP)), 'sell_acc:{}'.format(final_ans.TN/(final_ans.TN+final_ans.FN)))
            print(final_ans)





init_acc()
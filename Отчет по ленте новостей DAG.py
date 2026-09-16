import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from io import StringIO
import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


my_token = "my_token" # здесь токен бота в кавычках (бот создала самостоятельно)
bot = telegram.Bot(token=my_token)

chat_id = ####### id чата, куда отправляется отчет 

def ch_get_df(query='Select 1', host='http://clickhouse.lab.karpov.courses:8123', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


default_args = {
    'owner': 'e.hozhaj',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.combine(datetime.now()-timedelta(days=1), datetime.min.time()),
}


schedule_interval = '58 07 * * *' 

# Нужны метрики для отчета: DAU, Просмотры, Лайки, CTR
# текст с информацией о значениях ключевых метрик за предыдущий день
# график с значениями метрик за предыдущие 7 дней

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_metrics_khozhay():
    
    @task()
    def extract_metrics():
        query = """SELECT toDate(time) AS date,
                            count(DISTINCT user_id) AS DAU, 
                            sum(action = 'view') AS views,
                            sum(action = 'like') AS likes,
                            ROUND(sum(action = 'like') / sum(action = 'view') * 100, 2) AS CTR
                    FROM simulator_20260620.feed_actions
                    WHERE toDate(time) >= today() - INTERVAL 7 DAY
                    GROUP BY date
                    ORDER BY date
                    format TSVWithNames"""
        week_metrics = ch_get_df(query=query)
        return week_metrics
    
    @task()
    def transform_data(week_metrics):
        df_metrics = week_metrics.groupby('date')\
        .max()\
        .reset_index()
        return df_metrics
    
    @task()
    def transform_to_text(df_metrics):
        df_metrics['date'] = pd.to_datetime(df_metrics['date'])
        report_info = df_metrics[df_metrics['date'] == df_metrics['date'].max()]
        msg_report = (
            f"Отчет по метрикам за {df_metrics['date'].max().strftime('%d.%m.%Y')} по ленте:\n"
            f"DAU: {report_info.DAU.values[0]}\n"
            f"Просмотры: {report_info.views.values[0]}\n"
            f"Лайки: {report_info.likes.values[0]}\n"
            f"CTR: {report_info.CTR.values[0]}%\n"
        )
        
        print(msg_report)
        # bot.sendMessage(chat_id=chat_id, text=msg_report) - для отправки в тг
    
    @task()
    def transform_to_photo(df_metrics): 
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Метрики за предыдущие 7 дней', fontsize=16)
        axes = axes.flatten() 
        # чтобы пройтись по нему циклом

        metrics = ['views', 'likes', 'DAU', 'CTR']
        names = ['Количество просмотров', 'Количество лайков', 'Активные пользователи', '%']

        for i, metric in enumerate(metrics):
            sns.lineplot(data=df_metrics, x='date', y=metric, ax=axes[i], marker='o', color='royalblue')
            axes[i].set_title(f'{metric}', fontsize=12)
            axes[i].set_xlabel('Дата', fontsize=10)
            axes[i].set_ylabel(f'{names[i]}', fontsize=10) 
            axes[i].tick_params(axis='x', rotation=45)
            axes[i].grid(True, alpha=0.5)

        plt.tight_layout()
        plt.subplots_adjust(hspace=0.4, wspace=0.3) 
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics_weekly_report.png'
        plt.close()
        #bot.sendPhoto(chat_id=chat_id, photo=plot_object) - для отправки в тг
        return plot_object
    
    week_metrics = extract_metrics()
    df_metrics = transform_data(week_metrics)
    msg_report = transform_to_text(df_metrics)
    plot_report = transform_to_photo(df_metrics)
    
report_metrics_lenta_khozhay = report_metrics_khozhay()

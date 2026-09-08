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


my_token = "8602950487:AAHsR7Veo0xfcXkNVe1XO_YzjkW5gzGx5rk"
bot = telegram.Bot(token=my_token)

chat_id = -1002614297220 

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

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def app_metrics_khozhay():
    
    @task()
    def extract_data_feed():
        query = """SELECT toDate(time) AS date,
                        count(DISTINCT user_id) AS DAU, 
                        sum(action = 'view') AS views,
                        sum(action = 'like') AS likes,
                        count(DISTINCT post_id) as uniq_posts,
                        count(action) AS actions,
                        ROUND(sum(action = 'like') / sum(action = 'view') * 100, 2) AS CTR
                    FROM simulator_20260620.feed_actions
                    WHERE toDate(time) BETWEEN (today() - 13) AND (today() - 1)
                    GROUP BY date
                    ORDER BY date
                    format TSVWithNames"""
        feed = ch_get_df(query=query)
        return feed
    
    @task()
    def extract_data_message():
        query = """SELECT toDate(time) as date,
                            count (DISTINCT user_id) as DAU,
                            count (user_id) as messages_sent,
                            count (DISTINCT receiver_id) as users_received
                    FROM simulator_20260620.message_actions 
                    WHERE toDate(time) BETWEEN (today() - 13) AND (today() - 1)
                    GROUP BY date
                    format TSVWithNames"""
        messanger_df = ch_get_df(query=query)
        return messanger_df
    
    @task()
    def extract_wau_from_feed():
        query = """SELECT DATE_TRUNC('week', time)::DATE AS week_start_date,
                        COUNT(DISTINCT user_id) AS WAU
                    FROM simulator_20260620.feed_actions
                    WHERE toDate(time) <= today() - 1
                    GROUP BY week_start_date
                    format TSVWithNames"""
        wau = ch_get_df(query=query)
        return wau
    
    @task()
    def extract_wau_from_message():
        query = """SELECT DATE_TRUNC('week', time)::DATE AS week_start_date,
                            COUNT(DISTINCT user_id) AS WAU
                    FROM simulator_20260620.message_actions
                    WHERE toDate(time) <= today() - 1
                    GROUP BY week_start_date
                    format TSVWithNames"""
        wau_mess = ch_get_df(query=query)
        return wau_mess
    
    # этап преобразования данных, так как выдает ошибку с датами
    @task()
    def transform_fdata(feed):
        feed = feed.groupby('date')\
        .max()\
        .reset_index()
        
        feed['actions_per_user'] = (feed['actions']/feed['DAU']).round(1)
        return feed
    
    @task()
    def transform_mdata(messanger_df):
        messanger_df = messanger_df.groupby('date')\
        .max()\
        .reset_index()
        
        messanger_df["number_per_user"] = (messanger_df["messages_sent"]/messanger_df["DAU"]).round(1)
        return messanger_df
    
    @task()
    def transform_wau_data(wau):
        wau = wau.groupby('week_start_date')\
        .max()\
        .reset_index()
        return wau
    
    @task()
    def transform_wau_mess_data(wau_mess):
        wau_mess = wau_mess.groupby('week_start_date')\
        .max()\
        .reset_index()
        return wau_mess
    
    @task()
    def transform_to_report(feed, messanger_df, wau, wau_mess):
        # привожу к дате, так как все сбилось в airflow
        feed['date'] = pd.to_datetime(feed['date'])
        messanger_df['date'] = pd.to_datetime(messanger_df['date'])
        wau['week_start_date'] = pd.to_datetime(wau['week_start_date'])
        wau_mess['week_start_date'] = pd.to_datetime(wau_mess['week_start_date'])
        
        info_feed = feed[feed['date'] == feed['date'].max()]
        info_messanger = messanger_df[messanger_df['date'] == messanger_df['date'].max()]
        wau_2 = wau[wau['week_start_date'] == wau['week_start_date'].max()]
        wau_mess_2 = wau_mess[wau_mess['week_start_date'] == wau_mess['week_start_date'].max()]

        msg_report = (
            f"Отчет за {feed['date'].max().strftime('%d.%m.%Y')}:\n"

            f"Отчет по ленте новостей:\n"
            f"- DAU: {info_feed.DAU.values[0]/ 1000:.1f} тыс. пользователей\n"
            f"- Просмотры: {info_feed.views.values[0]/ 1000:.1f} тыс.\n"
            f"- Лайки: {info_feed.likes.values[0]/ 1000:.1f} тыс.\n"
            f"- CTR: {info_feed.CTR.values[0]}%\n"
            f"- Количество действий на одного пользователя: {info_feed.actions_per_user.values[0]}\n"
            f"- Количество просмотренных постов: {info_feed.uniq_posts.values[0]}\n"
            f"- WAU за неделю до: {wau_2.WAU.values[0]/ 1000:.1f} тыс. пользователей\n"

            f"Отчет по мессенджеру:\n"
            f"- DAU: {info_messanger.DAU.values[0]/ 1000:.1f} тыс. пользователей\n"
            f"- Количество отправленных сообщений: {info_messanger.messages_sent.values[0]/ 1000:.1f} тыс.\n"
            f"- Количество получателей сообщений: {info_messanger.users_received.values[0]} пользователей\n"
            f"- Количество отправленных сообщений на пользователя: {info_messanger.number_per_user.values[0]}\n"
            f"- WAU за неделю до: {wau_mess_2.WAU.values[0]/ 1000:.1f} тыс. пользователей\n"
        )

        print(msg_report)
    
    @task()
    def build_graph(feed, messanger_df):
        fig, axes = plt.subplots(3, 3, figsize=(15, 10))
        fig.suptitle('Метрики за предыдущие 14 дней', fontsize=16)
        axes = axes.flatten() 
        # чтобы пройтись по нему циклом
        feed_metrics = ['DAU', 'views', 'likes', 'CTR', 'actions_per_user', 'uniq_posts']
        metrics_messanger = ['DAU', 'users_received', 'number_per_user']

        for i, metric in enumerate(feed_metrics):
            sns.lineplot(data=feed, x='date', y=metric, ax=axes[i], marker='o', color='royalblue')
            axes[i].set_title(f'{metric}', fontsize=12)
            axes[i].set_xlabel('Дата', fontsize=10) 
            axes[i].tick_params(axis='x', rotation=45)
            axes[i].grid(True, alpha=0.5)

        for i, metric in enumerate(metrics_messanger):
            sns.lineplot(data=messanger_df, x='date', y=metric, ax=axes[len(feed_metrics) + i], marker='o',
                         color='royalblue') # Другой цвет для отличия
            axes[len(feed_metrics) + i].set_title(f'Мессенджер: {metric}', fontsize=12)
            axes[len(feed_metrics) + i].set_xlabel('Дата', fontsize=10)
            axes[len(feed_metrics) + i].tick_params(axis='x', rotation=45)
            axes[len(feed_metrics) + i].grid(True, alpha=0.5)

        plt.tight_layout()
        plt.subplots_adjust(hspace=0.4, wspace=0.3) 
        fig.autofmt_xdate(rotation=45, ha='right') #автоматическое форматирование дат на оси X
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics_report.png'
        plt.close()
        return plot_object
    
    @task()
    def build_wau_graph(wau, wau_mess):
        fig, axes = plt.subplots(1, 2, figsize=(15, 10))
        fig.suptitle('WAU за последние 2 месяца', fontsize=16)
        axes = axes.flatten() 

        wau_df = wau.merge(wau_mess, how="inner", on="week_start_date")
        wau_df = wau_df.rename(columns={'WAU_x': 'WAU_lenta', 
                               'WAU_y': 'WAU_messanger'})

        wau_metrics = ['WAU_lenta', 'WAU_messanger'] 

        for i, metric in enumerate(wau_metrics):
            sns.lineplot(data=wau_df, x='week_start_date', y=metric, ax=axes[i], marker='o', color='royalblue')
            axes[i].set_title(f'{metric}', fontsize=12)
            axes[i].set_xlabel('Дата', fontsize=10)
            axes[i].tick_params(axis='x', rotation=45)
            axes[i].grid(True, alpha=0.5)

        plt.tight_layout()
        plot_object_2 = io.BytesIO()
        plt.savefig(plot_object_2)
        plot_object_2.seek(0)
        plot_object_2.name = 'report_long_run.png'
        plt.close()
        return plot_object_2
    
    feed = extract_data_feed()
    messanger_df = extract_data_message()
    wau = extract_wau_from_feed()
    wau_mess = extract_wau_from_message()
    feed = transform_fdata(feed)
    messanger_df = transform_mdata(messanger_df)
    wau = transform_wau_data(wau)
    wau_mess = transform_wau_mess_data(wau_mess)
    msg_report = transform_to_report(feed, messanger_df, wau, wau_mess)
    plot_object = build_graph(feed, messanger_df)
    plot_object_2 = build_wau_graph(wau, wau_mess)
    
app_metrics_khozhay = app_metrics_khozhay()
    
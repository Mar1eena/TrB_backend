import pandas as pd
import clickhouse_connect
import backtrader as bt
import backtrader.analyzers as btanalyzers
import optuna

# 1. Создаем стандартную стратегию Backtrader
class SmaCrossStrategy(bt.Strategy):
    params = (('fast_period', 5), ('slow_period', 60),)

    def __init__(self):
        sma1 = bt.indicators.SMA(period=self.params.fast_period)
        sma2 = bt.indicators.SMA(period=self.params.slow_period)
        self.crossover = bt.indicators.CrossOver(sma1, sma2)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()

# 2. Целевая функция для Optuna
def objective(trial):
    # Подбираем оба периода. Задаем правило: slow всегда больше fast минимум на 5
    fast_p = trial.suggest_int('fast_period', 5, 20)
    slow_p = trial.suggest_int('slow_period', fast_p + 5, 30)
    
    cerebro = bt.Cerebro()
    
    # ИСПРАВЛЕНО: Передаем правильное имя класса стратегии SmaCrossStrategy и оба параметра
    cerebro.addstrategy(SmaCrossStrategy, fast_period=fast_p, slow_period=slow_p)
    
    # Берем данные из глобальной переменной, которая уже в памяти
    cerebro.adddata(DATA_FROM_CH) 
    
    cerebro.addanalyzer(btanalyzers.SharpeRatio_A, _name='sharpe')
    results = cerebro.run()
    
    # Извлекаем коэффициент Шарпа
    sharpe_ratio = results[0].analyzers.sharpe.get_analysis().get('sharperatio', None)
    
    # Если сделок не было или произошла ошибка расчетов, возвращаем штрафное значение
    if sharpe_ratio is None or str(sharpe_ratio) == 'nan':
        return -10.0
        
    return sharpe_ratio

# 3. Функция выгрузки данных
def get_data_from_clickhouse(uid: str) -> bt.feeds.PandasData:
    client = clickhouse_connect.get_client(
        host='localhost',      
        port=8123,             
        username='default',
        password='default',
        database='TrB'
    )
    
    query = f"""
        SELECT 
            time,
            open,
            high,
            low,
            close,
            volume
        FROM TrB.hct
        WHERE uid = '{uid}' and interval = 5
        ORDER BY time ASC
    """
    
    df = client.query_df(query)
    
    df['datetime'] = pd.to_datetime(df['time'])
    df.set_index('datetime', inplace=True)
    
    data_feed = bt.feeds.PandasData(
        dataname=df,
        datetime=None,  
        open='open',
        high='high',
        low='low',
        close='close',
        volume='volume',
        openinterest=-1 
    )
    
    return data_feed

# 4. Точка входа и запуск оптимизации
if __name__ == '__main__':
    # ИСПРАВЛЕНО: Передаем конкретный uid вашего торгового инструмента из таблицы hct
    TARGET_UID = '962e2a95-02a9-4171-abd7-aa198dbe643a' # Замените на ваш реальный uid (например, 'AAPL', 'EURUSD' и т.д.)
    
    print(f"Загрузка данных из ClickHouse для {TARGET_UID}...")
    DATA_FROM_CH = get_data_from_clickhouse(uid=TARGET_UID) 

    print("Запуск оптимизации Optuna...")
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=500)

    print("\nОптимизация завершена успешно!")
    print("Лучшие параметры:", study.best_params)
    print("Лучший коэффициент Шарпа:", study.best_value)

    fig1 = optuna.visualization.plot_param_importances(study)
    fig2 = optuna.visualization.plot_optimization_history(study)

    # Открываем их в браузере
    fig1.show()
    fig2.show()

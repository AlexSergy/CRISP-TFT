import requests
import zipfile
import pandas as pd
from pathlib import Path
import io
from tqdm import tqdm
import time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

class BinanceDataCollector:
    """
    Загружает исторические данные Binance через S3 API, распаковывает и объединяет в один CSV
    """
    
    def __init__(self, symbol, interval='4h'):
        self.symbol = symbol
        self.interval = interval
        self.temp_dir = Path(f'temp_{symbol}')
        self.temp_dir.mkdir(exist_ok=True)
        
        # AWS S3 endpoints для Binance
        self.s3_base = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
        
    def get_zip_links(self):
        """Получает список всех .zip файлов через S3 API"""
        print(f"\n📊 Получаю список файлов для {self.symbol}...")
        
        # Формируем запрос к S3 API
        prefix = f'data/spot/monthly/klines/{self.symbol}/{self.interval}/'
        url = f'{self.s3_base}?delimiter=/&prefix={prefix}'
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Парсим XML ответ от S3
            root = ET.fromstring(response.content)
            
            # Namespace для S3 XML
            ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            
            zip_links = []
            
            # Ищем все Contents элементы (файлы)
            for content in root.findall('s3:Contents', ns):
                key = content.find('s3:Key', ns)
                if key is not None and key.text.endswith('.zip'):
                    file_url = f'{self.s3_base}/{key.text}'
                    zip_links.append(file_url)
            
            # Если namespace не сработал, пробуем без него
            if not zip_links:
                for content in root.findall('Contents'):
                    key = content.find('Key')
                    if key is not None and key.text.endswith('.zip'):
                        file_url = f'{self.s3_base}/{key.text}'
                        zip_links.append(file_url)
            
            print(f"✅ Найдено {len(zip_links)} архивов")
            return sorted(zip_links)
            
        except Exception as e:
            print(f"❌ Ошибка при получении списка файлов: {e}")
            print(f"URL: {url}")
            
            # Запасной вариант: генерируем URLs по известному паттерну
            print("\n🔄 Пробую альтернативный метод (генерация URLs)...")
            return self.generate_urls_by_pattern()
    
    def generate_urls_by_pattern(self):
        """
        Генерирует URLs по известному паттерну Binance:
        SYMBOL-INTERVAL-YEAR-MONTH.zip
        """
        zip_links = []
        
        # Определяем диапазон дат
        # BTC торгуется с 2017, ETH примерно с 2017-2018
        start_year = 2017 if self.symbol == 'BTCUSDT' else 2018
        end_date = datetime.now()
        
        current = datetime(start_year, 1, 1)
        
        while current <= end_date:
            year = current.year
            month = str(current.month).zfill(2)
            
            filename = f'{self.symbol}-{self.interval}-{year}-{month}.zip'
            url = f'{self.s3_base}/data/spot/monthly/klines/{self.symbol}/{self.interval}/{filename}'
            
            zip_links.append(url)
            
            # Переходим к следующему месяцу
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
        
        print(f"✅ Сгенерировано {len(zip_links)} потенциальных URLs")
        return zip_links
    
    def download_and_extract(self, url, retries=3):
        """Скачивает и распаковывает один архив с повторными попытками"""
        for attempt in range(retries):
            try:
                # Скачиваем архив в память
                response = requests.get(url, timeout=60)
                
                # Если файл не найден (404), пропускаем
                if response.status_code == 404:
                    return None
                    
                response.raise_for_status()
                
                # Распаковываем
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    # Обычно в архиве один CSV файл
                    for filename in z.namelist():
                        if filename.endswith('.csv'):
                            # Извлекаем во временную директорию
                            z.extract(filename, self.temp_dir)
                            return self.temp_dir / filename
                
                return None
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 404:
                    if attempt < retries - 1:
                        time.sleep(2)  # Пауза перед повтором
                        continue
                    print(f"⚠️  HTTP ошибка при обработке {url}: {e}")
                return None
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2)  # Пауза перед повтором
                    continue
                print(f"⚠️  Ошибка при обработке {url}: {e}")
                return None
        
        return None
    
    def merge_csv_files(self, csv_files, output_filename):
        """Объединяет все CSV файлы в один"""
        print(f"\n🔄 Объединяю {len(csv_files)} файлов...")
        
        # Стандартные колонки Binance kline data
        columns = [
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ]
        
        dfs = []
        for csv_file in tqdm(csv_files, desc="Чтение файлов"):
            try:
                df = pd.read_csv(csv_file, names=columns, header=None)
                dfs.append(df)
            except Exception as e:
                print(f"⚠️  Ошибка чтения {csv_file}: {e}")
        
        if not dfs:
            print("❌ Нет данных для объединения")
            return None
        
        # Объединяем все данные
        merged_df = pd.concat(dfs, ignore_index=True)
        
        # Валидация и очистка timestamp'ов
        print("🔍 Проверяю корректность timestamp'ов...")
        initial_rows = len(merged_df)
        
        # Конвертируем в numeric, некорректные станут NaN
        merged_df['open_time'] = pd.to_numeric(merged_df['open_time'], errors='coerce')
        
        # Разумные границы для криптовалютных данных:
        # Min: 2009-01-01 (начало Bitcoin) = 1230768000000 ms
        # Max: текущая дата + 1 год для запаса
        min_timestamp = 1230768000000  # 2009-01-01
        max_timestamp = int((datetime.now() + timedelta(days=365)).timestamp() * 1000)
        
        # Фильтруем некорректные timestamp'ы
        valid_mask = (
            merged_df['open_time'].notna() & 
            (merged_df['open_time'] >= min_timestamp) & 
            (merged_df['open_time'] <= max_timestamp)
        )
        
        invalid_count = (~valid_mask).sum()
        if invalid_count > 0:
            print(f"⚠️  Найдено {invalid_count} некорректных timestamp'ов, удаляю...")
        
        merged_df = merged_df[valid_mask].copy()
        
        # Сортируем по времени
        merged_df = merged_df.sort_values('open_time').reset_index(drop=True)
        
        # Удаляем дубликаты
        before_dedup = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset='open_time', keep='first')
        duplicates_removed = before_dedup - len(merged_df)
        if duplicates_removed > 0:
            print(f"🔄 Удалено {duplicates_removed} дубликатов")
        
        # Конвертируем timestamp в datetime
        merged_df['datetime'] = pd.to_datetime(merged_df['open_time'], unit='ms', utc=True)
        
        # Переставляем колонки для удобства
        cols = ['datetime', 'open_time'] + [col for col in merged_df.columns if col not in ['datetime', 'open_time']]
        merged_df = merged_df[cols]
        
        # Сохраняем
        merged_df.to_csv(output_filename, index=False)
        
        print(f"\n✅ Данные сохранены в {output_filename}")
        print(f"📈 Всего записей: {len(merged_df):,} (отфильтровано {initial_rows - len(merged_df)} некорректных)")
        print(f"📅 Период: {merged_df['datetime'].min()} - {merged_df['datetime'].max()}")
        
        return merged_df
    
    def cleanup(self):
        """Удаляет временные файлы"""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            print(f"🧹 Временные файлы удалены")
    
    def collect(self, output_filename):
        """Основной метод: скачивает, распаковывает и объединяет данные"""
        print(f"\n{'='*60}")
        print(f"🚀 Начинаю сбор данных для {self.symbol}")
        print(f"{'='*60}")
        
        # Получаем список архивов
        zip_links = self.get_zip_links()
        
        if not zip_links:
            print("❌ Не найдено файлов для загрузки")
            return None
        
        # Скачиваем и распаковываем
        csv_files = []
        failed_urls = []
        print(f"\n⬇️  Скачиваю и распаковываю архивы...")
        
        successful = 0
        failed = 0
        
        for url in tqdm(zip_links, desc="Загрузка"):
            csv_file = self.download_and_extract(url)
            if csv_file:
                csv_files.append(csv_file)
                successful += 1
            else:
                failed += 1
                failed_urls.append(url)
            time.sleep(0.1)  # Небольшая пауза между запросами
        
        print(f"\n📊 Успешно: {successful}, Пропущено: {failed}")
        
        if failed_urls and failed <= 5:  # Показываем только если не слишком много
            print("\n⚠️  Не удалось загрузить:")
            for url in failed_urls:
                print(f"   - {url.split('/')[-1]}")
        
        if not csv_files:
            print("❌ Не удалось загрузить файлы")
            return None
        
        # Объединяем
        df = self.merge_csv_files(csv_files, output_filename)
        
        # Очищаем временные файлы
        self.cleanup()
        
        return df


def main():
    """
    Главная функция для сбора данных BTC и ETH
    """
    print("="*60)
    print("📊 BINANCE DATA COLLECTOR")
    print("📦 Сбор 4h klines для BTC и ETH")
    print("="*60)
    
    # Настройки
    pairs = [
        {
            'symbol': 'BTCUSDT',
            'output': 'BTCUSDT_4h_full.csv'
        },
        {
            'symbol': 'ETHUSDT',
            'output': 'ETHUSDT_4h_full.csv'
        }
    ]
    
    results = {}
    
    for pair in pairs:
        collector = BinanceDataCollector(
            symbol=pair['symbol'],
            interval='4h'
        )
        
        df = collector.collect(pair['output'])
        results[pair['symbol']] = df
        
        print("\n" + "="*60 + "\n")
    
    # Финальная статистика
    print("="*60)
    print("🎉 СБОР ДАННЫХ ЗАВЕРШЕН!")
    print("="*60)
    
    for symbol, df in results.items():
        if df is not None:
            print(f"\n{symbol}:")
            print(f"  📄 Файл: {symbol}_4h_full.csv")
            print(f"  📊 Записей: {len(df):,}")
            print(f"  📅 Период: {df['datetime'].min()} - {df['datetime'].max()}")
            print(f"  💾 Размер: ~{df.memory_usage(deep=True).sum() / 1024**2:.1f} MB в памяти")
    
    print("\n✨ Готово! Можно переходить к EDA и построению фичей.")


if __name__ == "__main__":
    main()

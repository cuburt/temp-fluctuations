import subprocess
import logging
import json
import sys
import argparse

try:
    import pandas as pd
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd


class WeatherDataAnalyser:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = self.load_data()

    def load_data(self) -> pd.DataFrame or None:
        try:
            df = pd.read_csv(self.file_path)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            if df['date'].isna().any():
                logging.warning("Some dates could not be parsed and were set to NaT")
            return df
        except FileNotFoundError:
            logging.error(f"File not found: {self.file_path}")
        except pd.errors.EmptyDataError:
            logging.error(f"No data: {self.file_path} is empty")
        except pd.errors.ParserError:
            logging.error(f"Parsing error: {self.file_path} is not a valid CSV")
        return None

    def get_highest_temperature_by_city(self, uom: str) -> dict:
        return self.df.groupby('city')[f'temperature_{uom}'].max().round(1).to_dict()

    def get_temperature_fluctuation(self, fluctuation_threshold: int, uom: str) -> list:
        fluctuation = self.df.groupby('city')[f'temperature_{uom}'].agg(lambda x: round(x.max() - x.min(), 1))
        return fluctuation[fluctuation > fluctuation_threshold].index.tolist()

    def get_average_temperature_by_city(self, uom: str) -> dict:
        return self.df.groupby('city')[f'temperature_{uom}'].mean().round(1).to_dict()

    def get_highest_temperature_by_date(self, uom: str) -> dict:
        highest_temp = self.df.loc[self.df.groupby('date')[f'temperature_{uom}'].idxmax()]
        return highest_temp.set_index(highest_temp['date'].astype('str'))['city'].to_dict()

    def get_overall_average_temperature(self, uom: str) -> float:
        return round(self.df[f'temperature_{uom}'].mean(), 1)

    def analyze(self, fluctuation_threshold: int = 20, uom: str = 'fahrenheit') -> dict or None:
        if self.df is None:
            return

        assert uom in ['celsius', 'fahrenheit'], "Invalid unit of measurement. Use 'celsius' or 'fahrenheit'"
        assert fluctuation_threshold > 0, "Fluctuation threshold must be greater than 0"

        return {
            "highest_temperature_by_city": self.get_highest_temperature_by_city(uom),
            "highest_temperature_by_date": self.get_highest_temperature_by_date(uom),
            "cities_with_high_fluctuation": self.get_temperature_fluctuation(fluctuation_threshold, uom),
            "city_averages": self.get_average_temperature_by_city(uom),
            "average_temperature": self.get_overall_average_temperature(uom)
        }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze weather data.')
    parser.add_argument('--file_path', type=str, default='weather_data.csv', help='Path to the weather data CSV file')
    parser.add_argument('--fluctuation_threshold', type=int, default=20, help='Temperature fluctuation threshold')
    parser.add_argument('--uom', type=str, choices=['celsius', 'fahrenheit'], default='fahrenheit', help='Unit of measurement for temperature')

    args = parser.parse_args()

    analyser = WeatherDataAnalyser(file_path=args.file_path)
    results = analyser.analyze(fluctuation_threshold=args.fluctuation_threshold, uom=args.uom)
    print(json.dumps(results, indent=4))

import sys
sys.path.append('/workspaces/Finance-dashbord')  
print('yoooooooooooo')
import requests
import json
from config import core, schema
import logging_config

# Initialisation de la configuration de logging
logger = logging_config.logger

class ExtractStock:

    """Class pour extraire les données de l'API Binance et de l'API KuCoin"""

    API_CONFIG  = schema.StocksConfig( **core.load_config().data["api_finance_config"] )
    logger.info("API config loaded from config file")
    
    def __init__(self):
        # Initialisation de la liste de symboles
        self.symbols = []

    def extract_symbols(self, url=API_CONFIG.url_symbols , timeout=API_CONFIG.timeout):
        
        """Méthode pour extraire tous les symboles de l'API Binance qui se terminent par USDT"""

        response = requests.get(url, timeout= timeout)
        # Vérification du code de statut de la réponse
        if response.status_code == 200:
            logger.info("extract_symbols : response status code is 200")

            # Conversion de la réponse en JSON
            response = json.loads(response.text)

            # Parcours des symboles de la réponse
            for symbol in response:
                for value in symbol.values():
                    # Ajout des symboles se terminant par USDT
                    if value.endswith("USDT"):
                        self.symbols.append(value)

        else : 
            raise Exception(f"extract_symbols : response status code {response.status_code}")
        
    def extract_data(self, url=API_CONFIG.url_data, timeout=API_CONFIG.timeout):
        
        """Méthode pour extraire les données de l'API KuCoin"""

        logger.info("extracting finance data")
        
        # Limitation à 100 symboles maximum
        self.symbols = list(set(self.symbols))[:5]

        # Parcours des symboles
        for symbol in self.symbols:
            response = requests.get(url + f"{symbol[:-4]}-USDT", timeout=timeout)

            # Vérification du code de statut de la réponse
            if response.status_code == 200:
                # Ajout des données au format JSON
                # data.append(json.loads(response.text)["data"])
                yield json.loads(response.text)["data"]

            else : 
                raise Exception(f"extract_data : response status code {response.status_code}")
            
        # return data


# Bloc principal
if __name__ == "__main__":
    
    # Création de l'objet ExtractStock
    extract_api = ExtractStock()
    # Extraction des symboles de Binance
    extract_api.extract_symbols()
    # Extraction des données de KuCoin pour les symboles extraits
    data = extract_api.extract_data()
    
    for i in data : 
        print(i)
        print('*' * 50)
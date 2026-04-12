from dotenv import load_dotenv

from sqlalchemy import create_engine
import os

load_dotenv()

class ConexaoBD:
    def __init__(self):
        self.user = os.getenv("DB_USER")
        self.senha = os.getenv("DB_PASSWORD")
        self.banco_de_dados = os.getenv("DB_NAME")
        self.host = os.getenv("DB_HOST")
        self.porta = os.getenv("DB_PORT")

        if not self.senha:
            raise ValueError("❌ Senha do banco não definida na variável de ambiente!")

        self.engine = None

    def conectar(self):
        if self.engine is None:
            self.engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.senha}@{self.host}:{self.porta}/{self.banco_de_dados}"
            )
        return self.engine
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
from models import drop_tables, create_tables, Publisher, Book, Shop, Stock, Sale

DSN = 'postgresql://postgres:...@localhost:5432/netology_db'
engine = sqlalchemy.create_engine(DSN)


class HomeWork:
    def __init__(self):
        self.Session = sessionmaker(bind=engine)
        self.session = self.Session()

    def hw_drop_tables(self):
        drop_tables(engine)

    def hw_create_tables(self):
        create_tables(engine)

    def close(self):
        self.session.close()

    def load_data_from_json(self):
        f = open("tests_data.json")
        data = json.load(f)
        for _ in data:
            if _["model"] == "publisher":
                publisher = Publisher(
                    name=_["fields"]["name"]
                )
                self.session.add(publisher)
                self.session.commit()
                pass
            elif _["model"] == "book":
                book = Book(
                    title=_["fields"]["title"],
                    id_publisher=_["fields"]["id_publisher"]
                )
                self.session.add(book)
                self.session.commit()
            elif _["model"] == "shop":
                shop = Shop(
                    name=_["fields"]["name"]
                )
                self.session.add(shop)
                self.session.commit()
            elif _["model"] == "stock":
                stock = Stock(
                    id_shop=_["fields"]["id_shop"],
                    id_book=_["fields"]["id_book"],
                    count=_["fields"]["count"]
                )
                self.session.add(stock)
                self.session.commit()
            elif _["model"] == "sale":
                sale = Sale(
                    price=_["fields"]["price"],
                    date_sale=_["fields"]["date_sale"],
                    count=_["fields"]["count"],
                    id_stock=_["fields"]["id_stock"]
                )
                self.session.add(sale)
                self.session.commit()

    def hw_drop_create_fill(self):
        hw.hw_drop_tables()
        hw.hw_create_tables()
        hw.load_data_from_json()

    def find_sales_by_publisher(self, publisher_name=""):
        query = self.session.query(
            Sale, Stock, Book, Publisher
        ).join(
            Stock, Sale.id_stock == Stock.id
        ).join(
            Book, Stock.id_book == Book.id
        ).join(
            Publisher, Book.id_publisher == Publisher.id
        ).filter(
            Publisher.name == publisher_name
        )

        for sale, stock, book, publisher in query:
            print(book, "|", publisher, "|", sale)


hw = HomeWork()

# hw.hw_drop_create_fill()
publisher = input("Введите название нужного издателя: ")
hw.find_sales_by_publisher(publisher)

hw.close()

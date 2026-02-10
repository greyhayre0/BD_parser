from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column, Mapped
from sqlalchemy import ForeignKey, String, Float, Integer, DateTime
from datetime import datetime

class Base(DeclarativeBase):
    pass

class Genre(Base):
    __tablename__ = "genre"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    books: Mapped[list["Book"]] = relationship("Book", back_populates="genre")

class Author(Base):
    __tablename__ = "author"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    books: Mapped[list["Book"]] = relationship("Book", back_populates="author")

class Book(Base):
    __tablename__ = "book"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    amount: Mapped[int] = mapped_column(Integer, default=0)
    genre_id: Mapped[int] = mapped_column(ForeignKey("genre.id"), nullable=False)
    author_id: Mapped[int] = mapped_column(ForeignKey("author.id"), nullable=False)
    genre: Mapped["Genre"] = relationship("Genre", back_populates="books")
    author: Mapped["Author"] = relationship("Author", back_populates="books")
    order_items: Mapped[list["OrderBook"]] = relationship("OrderBook", back_populates="book")

class City(Base):
    __tablename__ = "city"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    delivery_time_days: Mapped[int] = mapped_column(Integer, default=7)
    clients: Mapped[list["Client"]] = relationship("Client", back_populates="city")

class Client(Base):
    __tablename__ = "client"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), nullable=False)
    city: Mapped["City"] = relationship("City", back_populates="clients")
    orders: Mapped[list["Order"]] = relationship("Order", back_populates="client")

class Order(Base):
    __tablename__ = "order"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wishes: Mapped[str] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    client_id: Mapped[int] = mapped_column(ForeignKey("client.id"), nullable=False)
    client: Mapped["Client"] = relationship("Client", back_populates="orders")
    order_books: Mapped[list["OrderBook"]] = relationship("OrderBook", back_populates="order")
    order_steps: Mapped[list["OrderStep"]] = relationship("OrderStep", back_populates="order")

class OrderBook(Base):
    __tablename__ = "order_book"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    order_id: Mapped[int] = mapped_column(ForeignKey("order.id"), nullable=False)
    book_id: Mapped[int] = mapped_column(ForeignKey("book.id"), nullable=False)
    order: Mapped["Order"] = relationship("Order", back_populates="order_books")
    book: Mapped["Book"] = relationship("Book", back_populates="order_items")

class Step(Base):
    __tablename__ = "step"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(String(500))
    order_steps: Mapped[list["OrderStep"]] = relationship("OrderStep", back_populates="step")

class OrderStep(Base):
    __tablename__ = "order_step"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    start: Mapped[datetime] = mapped_column(DateTime)
    finish: Mapped[datetime] = mapped_column(DateTime)
    order_id: Mapped[int] = mapped_column(ForeignKey("order.id"))
    step_id: Mapped[int] = mapped_column(ForeignKey("step.id"))
    order: Mapped["Order"] = relationship("Order", back_populates="order_steps")
    step: Mapped["Step"] = relationship("Step", back_populates="order_steps")


'''
На случай создания
from sqlalchemy import create_engine
engine = create_engine('postgresql://username:password@localhost:5432/bookstore')

Base.metadata.create_all(engine)
'''

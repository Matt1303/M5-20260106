
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

Base = declarative_base()

class Customer(Base):
    """customer table - stores library member information"""
    __tablename__ = 'customers'
    
    customer_id = Column(Integer, primary_key=True)
    customer_name = Column(String(200), nullable=False)
    
    loans = relationship('Loan', back_populates='customer')
    
    def __repr__(self):
        return f"<Customer(id={self.customer_id}, name='{self.customer_name}')>"


class Book(Base):
    """book table - stores unique book titles in the library catalog"""
    __tablename__ = 'books'
    
    book_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(500), nullable=False, unique=True)
    
    loans = relationship('Loan', back_populates='book')
    
    def __repr__(self):
        return f"<Book(id={self.book_id}, title='{self.title}')>"


class Loan(Base):
    """loan table - tracks book checkout and return transactions"""
    __tablename__ = 'loans'
    
    loan_id = Column(Integer, primary_key=True)
    book_id = Column(Integer, ForeignKey('books.book_id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('customers.customer_id'), nullable=False)
    
    checkout_date = Column(Date, nullable=False)
    return_date = Column(Date, nullable=True)
    days_allowed = Column(Integer, default=14)
    days_borrowed = Column(Integer, nullable=True)
    is_overdue = Column(Boolean, default=False)
    days_overdue = Column(Integer, default=0)
    
    book = relationship('Book', back_populates='loans')
    customer = relationship('Customer', back_populates='loans')
    
    def __repr__(self):
        return f"<Loan(id={self.loan_id}, book_id={self.book_id}, customer_id={self.customer_id}, overdue={self.is_overdue})>"


def create_database(db_path='library_system.db'):
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()


if __name__ == "__main__":
    engine = create_database()
    print(f"Database created successfully: library_system.db")
    print("\nTables created:")
    for table in Base.metadata.tables.keys():
        print(f"  - {table}")

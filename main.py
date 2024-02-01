

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import create_engine, Column, String, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker
import asyncio

db_url_sync = "postgresql+psycopg2://..."
db_url_async = "postgresql+asyncpg://..."

sync_engine = create_engine(db_url_sync)
sync_session = sessionmaker(sync_engine)

async_engine = create_async_engine(db_url_async)
async_session = async_sessionmaker(async_engine)

RUN_ASYNC_CODE = True


test_data = [{'column_1': 'value_1', 'column_2': 'value_2', 'column_3': 'value_3'},
             {'column_1': 'value_4', 'column_2': 'value_5', 'column_3': 'value_6'},
             {'column_1': 'value_7', 'column_2': 'value_8', 'column_3': 'value_9'}]
#
# test_data = [{'column_1': 'value_1', 'column_3': 'value_3'},
#              {'column_1': 'value_4', 'column_3': 'value_6'},
#              {'column_1': 'value_7', 'column_2': 'value_8', 'column_3': 'value_9'}]

# test_data = [{'column_1': 'value_1', 'column_2': 'value_2', 'column_3': 'value_3'},
#              {'column_1': 'value_4', 'column_3': 'value_6'},
#              {'column_1': 'value_7', 'column_3': 'value_9'}]


Base = declarative_base()


def create_meta_data():
    Base.metadata.create_all(bind=sync_engine)


def drop_meta_data():
    Base.metadata.drop_all(bind=sync_engine)


class TestOrmModel(Base):

    __tablename__ = "test_orm_model"

    column_1 = Column(String, primary_key=True)
    column_2 = Column(String)
    column_3 = Column(String)


def insert_data_sync():
    with sync_session() as session, session.begin():
        query = insert(TestOrmModel).values(test_data)
        print(query)
        session.execute(query)


async def insert_data_async():
    async with async_session() as session, session.begin():
        query = insert(TestOrmModel).values(test_data)
        await session.execute(query)
        print(query)

def get_insert_data():
    with sync_session() as session:
        insert_data = session.execute(select(TestOrmModel.column_1,
                                             TestOrmModel.column_2,
                                             TestOrmModel.column_3)).all()
        return [obj._asdict() for obj in insert_data]


if __name__ == '__main__':
    drop_meta_data()
    create_meta_data()
    if not RUN_ASYNC_CODE:
        insert_data_sync()
    else:
        asyncio.run(insert_data_async())

insert_data = get_insert_data()
for line in insert_data:
    print(line)

assert insert_data == test_data





import glob
import os
import random
from datetime import datetime

from pyspark.sql import SparkSession
import shutil
import sys
from pathlib import Path
from pyspark.sql.functions import date_trunc

from pyspark.sql.functions import when, round
from pyspark.sql.types import FloatType, IntegerType, StringType

if __name__ == '__main__':
    store_name = 'store'
    parent_path = Path(__file__).parent.absolute()

    spark = SparkSession.builder.getOrCreate()
    for file_name in list(glob.glob(str(parent_path / 'jsons' / '*.json'))):
        print(f'processing {file_name}')
        df = spark.read.json(file_name)
        print(f'{df.count()} records read from json')

        df = df.drop('data-group-quantity', 'data-energy-drink', 'data-amount', 'data-item-weight', 'data-energy-drink'). \
            withColumn('old_price', df['data-old-price'].cast(FloatType())). \
            withColumn('old_price_per_kg', df['data-old-price-per-kg'].cast(FloatType())). \
            withColumn('price', df['data-price'].cast(FloatType())). \
            withColumn('price_per_kg', df['data-price-per-kg'].cast(FloatType())). \
            withColumn('product_id', df['data-product-id'].cast(IntegerType())). \
            withColumn('type', df['data-type'].cast(StringType())). \
            withColumn('weight', df['data-weight'].cast(IntegerType()))

        calc_discount = round((1 - df.price / df.old_price) * 100, 2)

        df = df.withColumn('discount', when(df.old_price.isNull(), None).otherwise(calc_discount)). \
            select('shop', 'name', 'weight', 'price', 'old_price', 'price_per_kg',
                   'old_price_per_kg', 'discount', 'cat', 'product_id', 'type', 'datetm', 'url')
        df.sort('name').show(5)

        try:
            store = spark.read.parquet(store_name)
        except Exception as e:
            if 'Path does not exist' in str(e):
                store = spark.createDataFrame([], df.schema)
            else:
                raise e

        store_count = store.count()
        print(f'{store_count} records read from in store')

        u = store.union(df)
        print(f'{u.count()} records after union')

        drop = u.withColumn('date', date_trunc('DAY', u.datetm)).dropDuplicates(['date', 'product_id', 'shop']).drop('date')
        print(f'{drop.count()}(+{drop.count()-store_count}) records saving to store (after drop duplicates by date product_id)')


        tmp_dir_name = '{}_tmp_{}{}'.format(store_name, datetime.now().strftime('%y_%d_%m__%H-%M-%S'),
                                            random.randint(000, 999))
        tmp_path = parent_path / tmp_dir_name
        drop.coalesce(1).write.parquet(str(tmp_path))
        shutil.rmtree(parent_path / store_name, ignore_errors=True)
        shutil.move(str(tmp_path), parent_path / store_name)

        os.rename(file_name, file_name + '.done')

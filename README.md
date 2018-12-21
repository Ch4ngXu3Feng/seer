# seer

## 数据获取

```
python seer.py --runner=scrapy --application=douban_movie --topic=subject --start_year=1938 --end_year=2018 --data_path=./data
```

## 数据整理

```
python seer.py --runner=transformer --application=douban_movie --topic=subject --data_path=./data
```

## 数据可视化

```
python seer.py
open http://localhost:9914
```
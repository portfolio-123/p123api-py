# Portfolio123 API Wrapper

Sample code:
```python
import p123api
with p123api.Client(api_id='your api id', api_key='your api key') as client:
    print(client.screen_run({'screen': {'type': 'stock', 'universe': 'nasdaq100'}, 'asOfDt': '2020-05-12'}))
except p123api.ClientException as e:
    print(e)
``` 

# OpenActive-py

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Reikyo/OpenActive-py/main?urlpath=%2Fapps%2Findex.ipynb)

## Directory contents

For all running options:
- cache/
- app.py

For running locally:
- requirements.txt

For running on Binder:
- environment.yml
- index.ipynb

For running on Heroku:
- Procfile
- requirements.txt
- runtime.txt

## Running locally

```
$ virtualenv virt
$ source virt/bin/activate
(virt) $ pip install -r requirements.txt
(virt) $ python app.py
    * Serving Flask app 'app'
    * Debug mode: off
    WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
    * Running on http://127.0.0.1:5000
```

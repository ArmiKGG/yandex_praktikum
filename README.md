run:
`docker-compose up`
then go to localhost:8080, login:pass - airflow:airflow, take a look at dag named `yandex_test` and tagged as `practikum` manually run or wait 3h :). it'll add data to mongodb cluster which I created for this job. <br>
<br>
For take a look at collected data run `take_a_look.py` (make sure you installed pymongo with dnspython) if not `pip install -r ./requirements.txt`
<br><br>
greetings!
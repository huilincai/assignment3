
# Lab - Basic RDD Operations


```python
spark
```





            <div>
                <p><b>SparkSession - hive</b></p>
                
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ip-172-31-32-104.ec2.internal:4043">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.1</code></dd>
              <dt>Master</dt>
                <dd><code>yarn</code></dd>
              <dt>AppName</dt>
                <dd><code>PySparkShell</code></dd>
            </dl>
        </div>
        
            </div>
        




```python
sc
```





        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ip-172-31-32-104.ec2.internal:4043">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.2.1</code></dd>
              <dt>Master</dt>
                <dd><code>yarn</code></dd>
              <dt>AppName</dt>
                <dd><code>PySparkShell</code></dd>
            </dl>
        </div>
        



Create an RDD called `A` that reads the following text file: `s3://bigdatateaching/shakespeare/100-0.txt`


```python
A = sc.textFile("s3://bigdatateaching/shakespeare/100-0.txt")
```


```python
A
```




    s3://bigdatateaching/shakespeare/100-0.txt MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0




```python
a = A.take(5)
```


```python
type(a)
```




    list




```python
a[1]
```




    u'Project Gutenberg\u2019s The Complete Works of William Shakespeare, by'




```python
A[0]
```


    ---------------------------------------------------------------------------

    TypeError                                 Traceback (most recent call last)

    <ipython-input-12-3f7632ffc2df> in <module>()
    ----> 1 A[0]
    

    TypeError: 'RDD' object does not support indexing



```python
A.count()
```




    147838




```python
A.cache()
```




    s3://bigdatateaching/shakespeare/100-0.txt MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0



The following python function 


```python
def hasHamlet( s ):
    return "Hamlet" in s
```


```python
b = A.filter(hasHamlet)
```


```python
b
```




    PythonRDD[10] at RDD at PythonRDD.scala:48




```python
b.count()
```




    106




```python
A.count()
```




    147838




```python
A.first(10)
```


    ---------------------------------------------------------------------------

    TypeError                                 Traceback (most recent call last)

    <ipython-input-24-75f8f03e9732> in <module>()
    ----> 1 A.first(10)
    

    TypeError: first() takes exactly 1 argument (2 given)



```python
b.first()
```




    u'CLAUDIUS, King of Denmark, Hamlet\u2019s uncle.'




```python
A.getNumPartitions()
```




    2




```python
b.takeSample(num = 10, withReplacement = False)
```




    [u'And that in Hamlet\u2019s hearing, for a quality',
     u' Enter Hamlet.',
     u'If Hamlet give the first or second hit,',
     u'Than a command to parley. For Lord Hamlet,',
     u'The GHOST of the late king, Hamlet\u2019s father.',
     u' Hamlet wounds Laertes._]',
     u'I have nothing with this answer, Hamlet; these words are not mine.',
     u' Enter Hamlet and Horatio, at a distance.',
     u' Enter Hamlet and Guildenstern.',
     u' [_Laertes wounds Hamlet; then, in scuffling, they change rapiers, and']




```python
sc.stop()
```


```python
q1 = sc.textFile("s3://bigdatateaching/quazyilx/quazyilx3.txt")

```


```python
q1
```




    s3://bigdatateaching/quazyilx/quazyilx3.txt MapPartitionsRDD[17] at textFile at NativeMethodAccessorImpl.java:0




```python
q1.getNumPartitions()
```




    589




```python
badrec = q1.filter(lambda bad:"fnard:-1 fnok:-1 cark:-1 gnuck:-1" in bad).cache()
```


```python
badrec.count()
```


    ---------------------------------------------------------------------------

    KeyboardInterrupt                         Traceback (most recent call last)

    <ipython-input-32-d338afda481e> in <module>()
    ----> 1 badrec.count()
    

    /usr/lib/spark/python/pyspark/rdd.py in count(self)
       1039         3
       1040         """
    -> 1041         return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()
       1042 
       1043     def stats(self):


    /usr/lib/spark/python/pyspark/rdd.py in sum(self)
       1030         6.0
       1031         """
    -> 1032         return self.mapPartitions(lambda x: [sum(x)]).fold(0, operator.add)
       1033 
       1034     def count(self):


    /usr/lib/spark/python/pyspark/rdd.py in fold(self, zeroValue, op)
        904         # zeroValue provided to each partition is unique from the one provided
        905         # to the final reduce call
    --> 906         vals = self.mapPartitions(func).collect()
        907         return reduce(op, vals, zeroValue)
        908 


    /usr/lib/spark/python/pyspark/rdd.py in collect(self)
        807         """
        808         with SCCallSiteSync(self.context) as css:
    --> 809             port = self.ctx._jvm.PythonRDD.collectAndServe(self._jrdd.rdd())
        810         return list(_load_from_socket(port, self._jrdd_deserializer))
        811 


    /usr/lib/spark/python/lib/py4j-0.10.4-src.zip/py4j/java_gateway.py in __call__(self, *args)
       1129             proto.END_COMMAND_PART
       1130 
    -> 1131         answer = self.gateway_client.send_command(command)
       1132         return_value = get_return_value(
       1133             answer, self.gateway_client, self.target_id, self.name)


    /usr/lib/spark/python/lib/py4j-0.10.4-src.zip/py4j/java_gateway.py in send_command(self, command, retry, binary)
        881         connection = self._get_connection()
        882         try:
    --> 883             response = connection.send_command(command)
        884             if binary:
        885                 return response, self._create_connection_guard(connection)


    /usr/lib/spark/python/lib/py4j-0.10.4-src.zip/py4j/java_gateway.py in send_command(self, command)
       1026 
       1027         try:
    -> 1028             answer = smart_decode(self.stream.readline()[:-1])
       1029             logger.debug("Answer received: {0}".format(answer))
       1030             if answer.startswith(proto.RETURN_MESSAGE):


    /usr/lib64/python2.7/socket.pyc in readline(self, size)
        449             while True:
        450                 try:
    --> 451                     data = self._sock.recv(self._rbufsize)
        452                 except error, e:
        453                     if e.args[0] == EINTR:


    KeyboardInterrupt: 



```python
badrec.take(5)
```


```python
print(badrec.count())
```


```python
W = sc.textFile("s3://bigdatateaching/forensicswiki/2012_logs.txt")
```


```python
import re
import datetime
date_re = re.compile("(\d\d/[a-zA-Z]+/\d\d\d\d)")
```


```python
def extract(line):
    m = date_re.search(line)
    if m:
        d = datetime.datetime.strptime(m.group(1),"%d/%b/%Y")
        return "{:04}-{:02}".format(d.year,d.month)
```


```python
dates = W.map( lambda line: [ extract( line ), 1 ])
```


```python
dates.take(10)
```




    [['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1],
     ['2012-01', 1]]




```python
dates.countByKey()
```




    defaultdict(int,
                {'2012-01': 1544100,
                 '2012-02': 1325030,
                 '2012-03': 1274061,
                 '2012-04': 1016456,
                 '2012-05': 1173380,
                 '2012-06': 1300250,
                 '2012-07': 1287187,
                 '2012-08': 1450426,
                 '2012-09': 1284945,
                 '2012-10': 1498895,
                 '2012-11': 1397343,
                 '2012-12': 1396198,
                 '2013-01': 1283})




```python
dates.cache()
```




    PythonRDD[25] at RDD at PythonRDD.scala:48




```python
count_by_dates = dates.countByKey()
```


```python

```




    defaultdict(int,
                {'2012-01': 1544100,
                 '2012-02': 1325030,
                 '2012-03': 1274061,
                 '2012-04': 1016456,
                 '2012-05': 1173380,
                 '2012-06': 1300250,
                 '2012-07': 1287187,
                 '2012-08': 1450426,
                 '2012-09': 1284945,
                 '2012-10': 1498895,
                 '2012-11': 1397343,
                 '2012-12': 1396198,
                 '2013-01': 1283})




```python
from operator import add
add_by_date = dates.reduceByKey(add)
```


```python
add_by_date
```




    PythonRDD[30] at RDD at PythonRDD.scala:48




```python
local_add_by_date = add_by_date.collect()
```


```python
add_by_date.collect()
```




    [('2012-09', 1284945),
     ('2012-08', 1450426),
     ('2012-03', 1274061),
     ('2012-10', 1498895),
     ('2012-02', 1325030),
     ('2012-11', 1397343),
     ('2012-01', 1544100),
     ('2012-12', 1396198),
     ('2012-07', 1287187),
     ('2012-06', 1300250),
     ('2013-01', 1283),
     ('2012-05', 1173380),
     ('2012-04', 1016456)]




```python
type(local_add_by_date)
```




    list




```python
local_add_by_date.print

```


      File "<ipython-input-1-3f8919060244>", line 1
        local_add_by_date.print
                              ^
    SyntaxError: invalid syntax



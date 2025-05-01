### Running Kafka First
### Running Producer Second
### Running MinIO Third
### Running Spark Last

1. brew install python@3.11 : Must use python <3.11

2. python3.11 -m venv .venv

3. source .venv/bin/activate

4. pip install -r requirements.txt

5. where python (to see the path for python 3.11 in .venv and assign to env variables)

6. export PYSPARK_PYTHON=/Users/phuc.nguyen/Documents/VSCodeDestination/capstone/spark/electric/.venv/bin/python3.11 : remember to place your path, this is just an example
   export PYSPARK_DRIVER_PYTHON=/Users/phuc.nguyen/Documents/VSCodeDestination/capstone/spark/electric/.venv/bin/python3.11

7. make sure using java 11 or below as Hadoop only supports <=11 : java -version -> if >11 please install java 11 and export JAVA_HOME = path to java 11 ,e.g export JAVA_HOME=$(/usr/libexec/java_home -v 11) for Mac OS

8. install slf4j-api-1.7.32 into venv if can't automatically installed : optinal
8.1. cd spark/electric
9. python3.11 finish/main.py

deactivate (to out of .venv)

10. 

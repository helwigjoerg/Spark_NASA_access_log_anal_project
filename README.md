Spark_NASA_access_log_project

to run sbt package 

spark-submit --master yarn --class  helwig.joerg.SparkLogFileNASA.LogFileAnalisisLocalApp  7pathToJAR /PathToInputFile


example log entrie structue
in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839



Spark_NASA_access_log This project is the final project part of the course in cloudaxlab for analysing NASA log file with spark Problem Statement Churning the logs of NASA Kennedy Space Center WWW server.

Dataset is located at /data/spark/project/NASA_access_log_Aug95.gz in CloudxLab HDFS.

Above dataset is access log of NASA Kennedy Space Center WWW server in Florida. The logs are an ASCII file with one line per request, with the following columns:

host - making the request. A hostname when possible, otherwise the Internet address if the name could not be looked up.

timestamp - in the format "DAY MON DD HH:MM:SS YYYY", where DAY is the day of the week, MON is the name of the month, DD is the day of the month, HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year. The timezone is -0400.

request url - Request URL.

HTTP reply code

bytes returned by the server

Note that from 01/Aug/1995:14:52:01 until 03/Aug/1995:04:36:13 there are no accesses recorded, as the Web server was shut down, due to Hurricane Erin.

Based on the above data, please answer following questions:

Q1: Write spark code to find out top 10 requested URLs along with count of number of times they have been requested (This information will help company to find out most popular pages and how frequently they are accessed)

Sample output

URL Count shuttle/missions/sts-71/mission-sts-71.html 549 shuttle/resources/orbiters/enterprise.html 145

Q2: Write spark code to find out top 5 hosts / IP making the request along with count (This information will help company to find out locations where website is popular or to figure out potential DDoS attacks)

Sample output

URL Count 192.168.78.24 219

Q3: Write spark code to find out top 5 time frame for high traffic (which day of the week or hour of the day receives peak traffic, this information will help company to manage resources for handling peak traffic load)

Q4: Write spark code to find out 5 time frames of least traffic (which day of the week or hour of the day receives least traffic, this information will help company to do production deployment in that time frame so that less number of users will be affected if some thing goes wrong during deployment)

Q5: Write spark code to find out unique HTTP codes returned by the server along with count (this information is helpful for devops team to find out how many requests are failing so that appropriate action can be taken to fix the issue)

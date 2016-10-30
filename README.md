English Illustruction
=====================
# ApacheKylin-OA-Struct
This project is created for the logcenter of OA by ApacheKylin.

##Build Cube
Like the previous one,This project also make ApacheKylin automatic.However,I think this one is more simple because it needn't to think of how to save when the segment is busy and it doesn't care the data before today.In this way,I think this one is easier than that one.

##Connect Hive
This project is designed to run regularly.It will check the `time.properties` everytime.It will know the maxtime of every table by the file and get the new MaxTime by querying the Hive.If it querys the data successfully,it will overwrite the time.properties.When it builds the cube ,the segment of cube betweens the max time of the previous one and the newest one.

##More Information
You can get more information by my CSDN blog.The following articles record the whole process and difficulties of the project.

[Apache Kylin在OA日志结构化中的应用](http://blog.csdn.net/blackenn/article/details/52749767)

中文说明
=======
#ApacheKylin-OA-Struct
这个项目通过ApacheKylin处理OA日志结构化。

##构建Cube
像之前那个项目，这个项目同样使得ApacheKylin自动化了.但是，我认为这个比那个更简单，因为它不需要考虑Segment的忙时等待设计，也不在乎今天之前的数据。就这点而言，所以我觉得这个比之前的那个更加的容易。

##连接Hive
这个项目被设计成定时的。它每次启动将会检测`time.properties`。 通过该文件，它能够知道上一次运行时每张表的最大的时间并且通过请求Hive得到新的每张表的最大时间。如果请求成功，它将会将新的最大时间覆盖写入至文件。当它构建cube的时候，cube的时间区间在上次最大时间与本次最大时间之间。

##更多信息
你能够通过我的CSDN博客获得更多的信息。下面的文章记录了这个项目的整个过程以及难点。

[Apache Kylin在OA日志结构化中的应用](http://blog.csdn.net/blackenn/article/details/52749767)


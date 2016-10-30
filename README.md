English Illustruction
=====================
# ApacheKylin-OA-Struct
This project is created for the logcenter of OA by ApacheKylin.

##Build Cube
Like the previous one,This project also make ApacheKylin automatic.However,I think this one is more simple because it needn't to think of how to save when the segment is busy and it doesn't care the data before today.In this way,I think this one is easier than that one.

##Connect Java
This project is designed to run regularly.It will check the `time.properties` everytime.It will know the maxtime of every table by the file and get the new MaxTime by querying the Hive.If it querys the data successfully,it will overwrite the time.properties.When it builds the cube ,the segment of the cube betweens the max time of the previous one and the newest one.

##More Information
You can get more information by my CSDN blog.The following articles record the whole process and difficulties of the project.

[Apache Kylin在OA日志结构化中的应用](http://blog.csdn.net/blackenn/article/details/52749767)

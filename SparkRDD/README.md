# CS226 Assignment 3: SparkRDD

## Steps to Run

### 1. Building the project
```sh
$ mvn package
```

### 2. Executing the program
```sh
$ spark-submit --class edu.ucr.cs.cs226.ppoth001.SparkRDD  <jar_file_location> <input_file_path>
```

or 

```sh
$ mvn exec:java -Dexec.mainClass=edu.ucr.cs.cs226.ppoth001.SparkRDD -Dexec.args="<input_file_path>"
```
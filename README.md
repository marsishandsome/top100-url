# Question
Calculate top 100 urls from 100GB url file using less than 1GB memory (url is less than 1K).

# Solution
## Step1: Hash
Read the file sequentially. For each line of url, take Hash(url)/1024
and store the value in 1024 small files. The size of each small file
is about 100M, due to the distribution of url and the hash function.

## Step2: HashMap statistics
For each small file, load it into memory and calculate the number of
times the url appears in a HashMap.

## Step3: Merge and Heap Sort
Merge all the HashMaps and select the top100 urls using MinHeap.

# Problems
There may be a data skew problem, which may result in a shuffle file size exceeding 1GB.
In this case, split the large file into small files based on Hash modulo mode,
until all the files are less than 200M.

# Optimization
- Mapper Side Aggregation (supported)
- Multi-thread (TODO)

# Steps
## 1. Compile
```
mvn clean package
```

## 2. Generate Test URL Data
```
# 100M
java -cp target/top100-url-1.0-SNAPSHOT.jar mars.URLGenerator data/gen100m 1000 200000 5 1024

# 1G
java -cp target/top100-url-1.0-SNAPSHOT.jar mars.URLGenerator data/gen1g 10000 2000000 5 1024

# 10G
java -cp target/top100-url-1.0-SNAPSHOT.jar mars.URLGenerator data/gen10g 100000 20000000 5 1024

# 100G
java -cp target/top100-url-1.0-SNAPSHOT.jar mars.URLGenerator data/gen100g 1000000 200000000 5 1024
```

## 3. Calculate Top 100 URL
```
java -Xms1g -Xmx1g -cp target/top100-url-1.0-SNAPSHOT.jar mars.TopURL data/gen100m data/result100m

java -Xms1g -Xmx1g -cp target/top100-url-1.0-SNAPSHOT.jar mars.TopURL data/gen1g data/result1g

java -Xms1g -Xmx1g -cp target/top100-url-1.0-SNAPSHOT.jar mars.TopURL data/gen10g data/result10g

java -Xms1g -Xmx1g -cp target/top100-url-1.0-SNAPSHOT.jar mars.TopURL data/gen100g data/result100g
```

# Performance Test

## Hardware
```
CPU: Intel(R) Xeon(R) Gold 6148 CPU @ 2.40GHz
Core Number: 8
System: Centos 7 in Virtual Machine
```

## Result
| data size | time  |
| --------- | ----- |
| 0.1g      | 1s    |
| 1g        | 4s    |
| 10g       | 73s   |
| 100g      | 1902s |


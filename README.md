Adaptive Join Order Selection for Continuous Queries.

## Publication
Xinyi Ye, Xiangyang Gou, Lei Zou, and Wenjie Zhang. 2025. AJOSC: Adaptive Join Order Selection for Continuous Queries. Proc. ACM Manag. Data 3, 3 (SIGMOD), Article 126 (June 2025), 27 pages. https://doi.org/10.1145/3725263

## Compile
```
mkdir build
cd build
cmake ..
make -j
```
## Run

`./ours ${schema_filename} ${query_filename} ${static_stream_filename} ${dynamic_stream_filename} ${primary_key_filename} -l ${#updates_handled_before_start_compute_order_using_AJOSC}`

The first 5 parameters are file names; the last parameter is an integer.

## Data
### Format
#### Schema
The schema file contains multiple lines, where every line contains the attribute list of a table. 

`${table_name} ${attribute_num} ${list of attributes} `

For example, 

`complete_cast 4 id movie_id subject_id status_id `

#### Query

The query file is composed of 3 parts. The first part contains two lines.
```
${the number of tables in the query}
${list of tables}
```
The second part contains $1+|V|$ lines. Note that $V$ is a multiset, so `${|V|}` in this part might be larger than `${the number of tables in the query}` in the first part. 
```
${|V|}
0 ${table name}
1 ${table name}
...
```
The third part contains join conditions. Every line is a join condition. Every line looks like
`${T1.id} ${attribute1} ${T2.id} ${attribute2}`, which means $T_1.attribute_1=T_2.attribute_2$.

For example, 
```
5
movie_info_idx company_type title info_type movie_companies 
5
0 movie_info_idx
1 title
2 movie_companies
3 company_type
4 info_type

3 id 2 company_type_id
1 id 2 movie_id
1 id 0 movie_id
2 movie_id 0 movie_id
4 id 0 info_type_id
```

#### Update stream

Multiple lines. Every line is an update operation:

`${1 means insert, 0 means delete} ${table_name} ${attribute values}`

For example, 

`1 cast_info 30848237,528031,1677244,,,,9,`

#### Primary key
The primary key file contains multiple lines. Every line shows the primary key of a table. Every line looks like

`${table_name} ${# attributes in the primary key} ${attribute list}`

For example, 

`comp_cast_type 1 id`

### Dataset
JOB. https://github.com/gregrahn/join-order-benchmark
LDBC-SNB. https://github.com/ldbc/ldbc_snb_datagen_spark
JCC-H. https://github.com/ldbc/dbgen.JCC-H

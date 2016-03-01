# Spark to Tableau

The goal of this project is to give Spark the ability to generate Tableau Data Extract (.tde) files from Spark DataFrames.

Tableau data extracts is a columnar store format used by Tableau Software. Columnar databases store column values together rather than row values. As a result, they dramatically reduce the input/output required to access and aggregate the values in a column.

Read more at http://www.tableau.com/about/blog/2014/7/understanding-tableau-data-extracts-part1


## Instaling Tableau SDK

The Spark to Tableau lib depends on the Tableau SDK to generate the tde files. It uses a native API

http://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm#SDK/tableau_sdk.htm

## Using Spark to Tableau
To save your dataframe to tde, first import ```TableauDataFrame``` implicity:

```scala
import TableauDataFrame._
```

then call ```saveToTableau()``` method within your dataframe:

```scala
df.saveToTableau("content.tde")
```

Example with parquet:
```scala
import TableauDataFrame._
val df = sql.read.parquet("/mydata")
df.saveToTableau("mydata.tde")
```

Example creating your own DataFrame:

```scala
import TableauDataFrame._
val jsonRDD = sc.parallelize(Seq("""
      { "isActive": false,
        "balance": 1431.73,
        "picture": "http://placehold.it/32x32",
        "age": 35,
        "eyeColor": "blue"
      }""",
       """{
        "isActive": true,
        "balance": 2515.60,
        "picture": "http://placehold.it/32x32",
        "age": 34,
        "eyeColor": "blue"
      }""",
      """{
        "isActive": false,
        "balance": 3765.29,
        "picture": "http://placehold.it/32x32",
        "age": 26,
        "eyeColor": "blue"
      }""")
    )
val df = sql.read.parquet(jsonRDD)
df.saveToTableau("users.tde")
```

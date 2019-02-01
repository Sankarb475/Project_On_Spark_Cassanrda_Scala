CREATE KEYSPACE IF NOT EXISTS SparkToCassandra WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;

CREATE TABLE SparkToCassandra.AdultDataTable (
    age int,
    employer text,
    salary int,
    education text,
    educationyear int,
    colour text,
    country text,
    familystatus text,
    gender text,
    jobdesignation text,
    maritalstatus text,
    points1 int,
    points2 int,
    points3 int,
    salarytype text,
    PRIMARY KEY (age, employer, salary, education, educationyear)
);

CREATE TABLE SparkToCassandra.CountryWiseSalaryTable (
    salary int,
    country text,
    PRIMARY KEY (country)
);


CREATE TABLE SparkToCassandra.CountryWiseBachelorTable (
    country text,
    counting int,
    PRIMARY KEY (country)
);


CREATE TABLE SparkToCassandra.CountryWiseAvgAgeTable (
    age int,
    country text,
    PRIMARY KEY (country)
);


CREATE TABLE SparkToCassandra.DivorceDataTable (
    avgage int,
    country text,
    maritalstatus text,
    gender text,
    PRIMARY KEY (country)
);



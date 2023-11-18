# Aritzia case assignment

## Scenario 

- Aritzia provides Everyday Luxury for our customers. The
speed of packaging our high-quality products and materials
is an important part of this. To continue to provide everyday
luxury service to our customers, we need the ability to
measure and model our packing performance in our
distribution centers (DC).
- We want to calculate a pack KPI for each worker at our DC by
measuring the time it took them to pack each transaction and
compare this to how long it should have taken them based
on the Packing Time Standards.
- The business requires an overview and visibility on key
metrics.

## Data provided

- Transactions
- A list of orders each user packed
- Packing transaction are of type PKOCLOSE
- Article Master
- A list of articles with their respective departments
- Packing Time Standards
- Time standard for each operation at the packing
station

The data is simulated. Its structure resembles the typical data
measured in packaging.

## Assignment

• Please design a data model, script the necessary
queries, and present the following:

- Technical data model
- Consideration made in the model design
- Steps
- Risks
- SQL Queries
- Results: Users’ Pack KPIs with Actuals and
Targets

The sections above are not exhaustive. Add sections as
you deem necessary to tell the story of the data and your
findings. Describe all assumptions in the presentation.

## Environment and setup

- I've loaded the raw data as `seeds` into dbt. It is necessary to run the `dbt seed` command initially, in order to materialize the datasources.
- I've used the `dbt_utils` package. It is necessary to run `dbt deps` once in order for the macro used to run. 
- The code was written using Redshift SQL syntax, some functions may not run as expected on other databases.
- I've created a `staging-silver` folder for data models I consider to be intermediate. These are data transaformations that represent reusable business logic, but are not intended to be use as-is for reporting
- Production-ready models for reporting are in the `presentation-gold` folder.

## Assumptions about the data 

- The time standards are in seconds
- All the "basic" operations in the time standards need to occur once each, per order 
- All the "article" operations in the time standards need to occur once each per item, in addition to the basic operations of the order
- The time it took to pack was the time between one packing transaction and the next one, for a given user
- As such, we cannot estimate the pack time for the last order of a shift
- Because an entire order has the same timestamp, we can only calculate the time to pack an entire order, not a given article
- This model does not take into account the fact that the user needs to take breaks, break expectations should be added based on the employer's specifications

## Staging models created 

### dim_articles

This model matches the raw data, with null columns removed.

### dim_transactions

- Removed transactions with an invalid `quantity` field
- Renamed columns that were using protected terms
- Cast string columns to `int` or `date` where required

### lookup_packing_article_standards

This model represents the time standards for packing specific articles
- Unpivoted the time standards table in order to have a more usable table format
- Some string manipulations in order to be able to join on the `articles` table 
- Only included the rows that represent articles being packed 

### lookup_packing_process_standards

This model represents the time standards for packing operations
- Some string manipulation to make the operation name more readable

## Production model created 

### packing_timespent_byuser_byorder

This model captures how long each order took to pack, as well as the expected packing time based on time standards.
The output table is ready to connect to a data visualization tool such as Tableau.
Columns includes:

- pack_date
- user_id
- order_number
- time_spent_packing: the actual time spent packing the order 
- time_expected: the expected time to pack this order, based on time standards
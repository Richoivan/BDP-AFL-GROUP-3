# Data folder

This pipeline uses the **real RetailRocket E-commerce dataset**.

**Download from:** <https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset>

Place the file `events.csv` directly in this folder so the final path is:

    data/events.csv

The pipeline **will not start** without this file - the producer and the
batch job both fail fast with an instructive error message if it is
missing.

## Schema of events.csv

    timestamp,visitorid,event,itemid,transactionid

Where `event` is one of: `view`, `addtocart`, `transaction`.

Roughly 2.7M rows.

## Other RetailRocket files (not required)

The full Kaggle download also contains `item_properties_part1.csv`,
`item_properties_part2.csv`, and `category_tree.csv`. **They are not used
by this project** - the batch insight (top viewed items) and the
real-time metric (events per minute) are computed entirely from
`events.csv`. You can ignore those files.
